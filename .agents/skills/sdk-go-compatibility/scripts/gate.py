#!/usr/bin/env python3
"""Validate a reviewed compatibility matrix against fresh source inventories.

This checks recorded evidence and log integrity; it does not infer test assertions
or independently certify that a test proves compatibility.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys


FIELD_CASES = ("default", "explicit", "invalid", "unset", "interaction")
REVIEWS = ("inventory_completeness", "build_variants", "configuration_paths")
GOOD = {"compatible", "adapted", "delegated"}
STATES = GOOD | {"pending", "unverified", "unsupported", "needs_decision", "excluded"}


def read(path):
    with open(path) as f:
        return json.load(f)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def identity(inv):
    # Paths and git status differ between developer machines and CI. Content,
    # revisions, inventory, unresolved issues, and dependency roots must agree.
    obj = {
        "schema": inv["schema"],
        "snapshots": [{k: s[k] for k in ("role", "revision", "digest")}
                      for s in inv["snapshots"]],
        "items": sorted((i["id"], i["fingerprint"]) for i in inv["items"]),
        "issues": sorted(inv["issues"]),
        "external_references": sorted(inv["external_references"]),
    }
    return sha(json.dumps(obj, sort_keys=True).encode())


def init(args):
    inv = read(args.inventory)
    data = {
        "schema": 1,
        "baseline_identity": identity(inv),
        "candidate_identity": identity(read(args.candidate_inventory)) if args.candidate_inventory else "",
        "reviews": {k: "" for k in REVIEWS},
        "external_reviews": {},
        "runs": {},
        "entries": [dict(id=i["id"], fingerprint=i["fingerprint"],
                         status="pending", target="", trace="", reason="",
                         evidence=[], cases={}) for i in inv["items"]],
    }
    with open(args.matrix, "x") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    print(f"Created {len(data['entries'])} pending entries in {args.matrix}")


def check(args):
    inv, candidate, matrix = read(args.inventory), read(args.candidate_inventory), read(args.matrix)
    errors = []
    baseline_id, candidate_id = identity(inv), identity(candidate)
    matrix_dir = Path(args.matrix).resolve().parent

    for name, obj in (("baseline", inv), ("candidate", candidate), ("matrix", matrix)):
        if obj.get("schema") != 1:
            errors.append(f"{name}: unsupported schema")
    for name, obj in (("baseline", inv), ("candidate", candidate)):
        errors.extend(f"{name} inventory issue: {issue}" for issue in obj["issues"])
        if not obj["items"] or not obj["snapshots"]:
            errors.append(f"{name}: empty inventory or snapshots")
    if matrix.get("baseline_identity") != baseline_id:
        errors.append("baseline changed: review inventory drift and refresh evidence")
    if matrix.get("candidate_identity") != candidate_id:
        errors.append("candidate changed: refresh implementation evidence")
    for review in REVIEWS:
        if not matrix.get("reviews", {}).get(review, "").strip():
            errors.append(f"missing human review evidence: {review}")
    external = set(inv["external_references"]) | set(candidate["external_references"])
    for ref in sorted(external):
        if not matrix.get("external_reviews", {}).get(ref, "").strip():
            errors.append(f"unreviewed external type: {ref}")

    items = {i["id"]: i for i in inv["items"]}
    if len(items) != len(inv["items"]):
        errors.append("duplicate baseline inventory IDs")
    entries = {}
    counts = Counter()
    for entry in matrix.get("entries", []):
        key = entry["id"]
        if key in entries:
            errors.append(f"duplicate matrix entry: {key}")
        entries[key] = entry
        counts[entry.get("status", "pending")] += 1
    errors.extend(f"missing matrix entry: {k}" for k in sorted(items.keys() - entries.keys()))
    errors.extend(f"stale/unknown matrix entry: {k}" for k in sorted(entries.keys() - items.keys()))

    checked_runs = {}

    for run_id, run in matrix.get("runs", {}).items():
        if run.get("exit_code") != 0 or run.get("result") != "passed":
            errors.append(f"run {run_id}: recorded regression run did not pass")
        if not run.get("tests") or any(v != "passed" for v in run["tests"].values()):
            errors.append(f"run {run_id}: empty, failed, or skipped test results")

    def check_evidence(refs, label):
        if not isinstance(refs, list) or not refs:
            errors.append(f"{label}: missing executed test evidence")
            return
        for ref in refs:
            run_id, test_id = ref.get("run"), ref.get("test")
            run = matrix.get("runs", {}).get(run_id)
            if not run:
                errors.append(f"{label}: unknown run {run_id}")
                continue
            if run_id not in checked_runs:
                checked_runs[run_id] = True
                if run.get("baseline_identity") != baseline_id or run.get("candidate_identity") != candidate_id:
                    errors.append(f"run {run_id}: stale source identities")
                for field in ("command", "cwd", "toolchain", "environment", "log", "log_sha256"):
                    if not str(run.get(field, "")).strip():
                        errors.append(f"run {run_id}: missing {field}")
                if run.get("exit_code") != 0 or run.get("result") != "passed":
                    errors.append(f"run {run_id}: failed, skipped, or not executed")
                log = matrix_dir / run.get("log", "")
                if not log.is_file() or sha(log.read_bytes()) != run.get("log_sha256"):
                    errors.append(f"run {run_id}: missing or altered log artifact")
            if not test_id or run.get("tests", {}).get(test_id) != "passed":
                errors.append(f"{label}: test {test_id!r} did not pass in run {run_id}")

    for key, entry in entries.items():
        if key not in items:
            continue
        item = items[key]
        if entry.get("fingerprint") != item["fingerprint"]:
            errors.append(f"{key}: changed declaration/contract requires review")
        status = entry.get("status")
        if status not in STATES:
            errors.append(f"{key}: invalid status {status!r}")
        if status == "excluded":
            decision = entry.get("decision", {})
            if not decision.get("reference") or not decision.get("quote"):
                errors.append(f"{key}: exclusion lacks an explicit user decision reference and quote")
            if not args.allow_exclusions:
                errors.append(f"{key}: exclusion blocks full compatibility")
            continue
        if status not in GOOD:
            errors.append(f"{key}: unresolved status {status}")
            continue
        for field in ("target", "trace", "reason"):
            if not entry.get(field, "").strip():
                errors.append(f"{key}: missing {field}")
        if status == "adapted":
            decision = entry.get("decision", {})
            if not entry.get("mechanical_basis") and not (decision.get("reference") and decision.get("quote")):
                errors.append(f"{key}: adaptation needs a mechanical basis or explicit user decision")
        check_evidence(entry.get("evidence"), key)
        if item["kind"] in ("field", "embedded-field"):
            for case in FIELD_CASES:
                record = entry.get("cases", {}).get(case, {})
                if record.get("not_applicable", "").strip():
                    if case in ("default", "explicit"):
                        errors.append(f"{key}: {case} must have behavioral test evidence")
                else:
                    check_evidence(record.get("evidence"), f"{key} case {case}")

    print("Matrix: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    if errors:
        print(f"FAIL: {len(errors)} compatibility gate errors", file=sys.stderr)
        for e in errors[:80]:
            print(f"- {e}", file=sys.stderr)
        if len(errors) > 80:
            print(f"- ... {len(errors)-80} more", file=sys.stderr)
        return 1
    if counts["excluded"]:
        print(f"PASS for explicitly approved scope only; {counts['excluded']} exclusions remain")
    else:
        print("PASS: complete matrix with current recorded evidence; review semantic assertions separately")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("init", "check"):
        cmd = commands.add_parser(name)
        cmd.add_argument("--inventory", required=True)
        cmd.add_argument("--matrix", required=True)
        cmd.add_argument("--candidate-inventory", required=name == "check")
        if name == "check":
            cmd.add_argument("--allow-exclusions", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "init":
            init(args)
            return 0
        return check(args)
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as e:
        print(f"Invalid compatibility input: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
