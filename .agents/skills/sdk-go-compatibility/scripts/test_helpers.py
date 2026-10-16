"""Self-contained regression checks for inventory and gate behavior.

Run with python3 test_helpers.py. Fixtures and evidence are synthetic and never
constitute SDK compatibility evidence. Requires Go on PATH, no third-party libs.
"""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("gate", HERE / "gate.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class HelpersTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="sdk-go-compatibility-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def write(self, path, text):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def inventory(self, root, output, *deps):
        command = ["go", "run", str(HERE / "inventory.go"), "-root", str(root), "-out", str(output)]
        for dep in deps:
            command.extend(["-dependency", str(dep)])
        return subprocess.run(command, capture_output=True, text=True)

    def test_alias_nested_fields_plugins_and_variants(self):
        upstream = self.root / "upstream"
        self.write(upstream / "go.mod", "module example.test/sdk\ngo 1.23\n")
        self.write(upstream / "internal/impl.go", '''package internal
type Retry struct { Attempts int; Delay int }
type Options struct { Enabled bool; Retry *Retry; Nested struct { Value int } }
type Plugin interface { Configure(Options) error }
type Implementation struct{}
func (*Implementation) Start() error { return nil }
''')
        self.write(upstream / "worker/worker.go", '''package worker
import "example.test/sdk/internal"
type Options = internal.Options
type Plugin = internal.Plugin
type Implementation = internal.Implementation
''')
        self.write(upstream / "worker/platform_linux.go", '''//go:build linux
package worker
type PlatformOptions struct { Linux bool }
''')
        self.write(upstream / "worker/platform_windows.go", '''//go:build windows
package worker
type PlatformOptions struct { Windows bool }
''')
        self.write(upstream / "contrib/nested/go.mod", "module example.test/plugin/v2\ngo 1.23\n")
        self.write(upstream / "contrib/nested/plugin.go", "package special\ntype Configuration struct { Active bool }\n")
        candidate = self.root / "candidate"
        self.write(candidate / "go.mod", "module example.test/poc\ngo 1.23\n")
        self.write(candidate / "worker/worker.go", '''package worker
import (
  "example.test/sdk/worker"
  "example.test/plugin/v2"
)
type Options = worker.Options
type Plugin = worker.Plugin
type Extra = special.Configuration
''')
        out = self.root / "inventory.json"
        result = self.inventory(candidate, out, upstream)
        self.assertEqual(result.returncode, 0, result.stderr)
        inv = json.loads(out.read_text())
        self.assertEqual(inv["issues"], [])
        names = {i["id"].split("@")[0] for i in inv["items"]}
        self.assertIn("example.test/poc/worker.Options.Enabled", names)
        self.assertIn("example.test/poc/worker.Options.Retry.Attempts", names)
        self.assertIn("example.test/poc/worker.Options.Nested.Value", names)
        self.assertIn("example.test/poc/worker.Plugin.Configure", names)
        self.assertIn("example.test/poc/worker.Extra.Active", names)
        self.assertFalse(any(n.startswith("example.test/sdk/internal") for n in names))
        base = self.root / "base.json"
        self.assertEqual(self.inventory(upstream, base).returncode, 0)
        base_inv = json.loads(base.read_text())
        base_names = {i["id"].split("@")[0] for i in base_inv["items"]}
        self.assertIn("example.test/sdk/worker.PlatformOptions.Linux", base_names)
        self.assertIn("example.test/sdk/worker.PlatformOptions.Windows", base_names)
        self.assertIn("example.test/sdk/worker.Implementation.Start", base_names)
        before = gate.identity(inv)
        self.write(upstream / "internal/impl.go", (upstream / "internal/impl.go").read_text().replace("Enabled bool", "Enabled bool; Added int"))
        next_out = self.root / "next.json"
        self.assertEqual(self.inventory(candidate, next_out, upstream).returncode, 0)
        after = json.loads(next_out.read_text())
        self.assertNotEqual(before, gate.identity(after))
        self.assertTrue(any("Options.Added@" in i["id"] for i in after["items"]))
        self.assertNotEqual(self.inventory(candidate, out, upstream).returncode, 0)

    def test_unresolved_external_alias_is_visible(self):
        source = self.root / "source"
        self.write(source / "go.mod", "module example.test/sdk\ngo 1.23\n")
        self.write(source / "options.go", 'package sdk\nimport "missing.test/config"\ntype Options = config.Options\n')
        out = self.root / "inventory.json"
        self.assertEqual(self.inventory(source, out).returncode, 0)
        inv = json.loads(out.read_text())
        self.assertTrue(any("unresolved alias" in issue for issue in inv["issues"]))

    def test_gate_rejects_decisions_drift_skips_and_missing_cases(self):
        inv = {
            "schema": 1, "snapshots": [{"role": "primary", "revision": "fixture", "digest": "fixture"}],
            "items": [{"id": "worker.Options.Enabled", "kind": "field", "fingerprint": "fixture"}],
            "issues": [], "external_references": [],
        }
        identity = gate.identity(inv)
        log = self.root / "run.log"
        log.write_text("SYNTHETIC FIXTURE: passed named test\n")
        ref = {"run": "synthetic", "test": "TestOption"}
        entry = {
            "id": "worker.Options.Enabled", "fingerprint": "fixture", "status": "compatible",
            "target": "fixture", "trace": "fixture", "reason": "fixture", "evidence": [ref],
            "cases": {k: {"evidence": [ref]} for k in gate.FIELD_CASES},
        }
        matrix = {
            "schema": 1, "baseline_identity": identity, "candidate_identity": identity,
            "reviews": {k: "synthetic fixture" for k in gate.REVIEWS}, "external_reviews": {},
            "entries": [entry], "runs": {"synthetic": {
                "baseline_identity": identity, "candidate_identity": identity,
                "command": "synthetic fixture", "cwd": str(self.root), "toolchain": "fixture",
                "environment": "synthetic", "exit_code": 0, "result": "passed", "log": "run.log",
                "log_sha256": gate.sha(log.read_bytes()), "tests": {"TestOption": "passed"},
            }},
        }
        baseline_path, matrix_path = self.root / "baseline.json", self.root / "matrix.json"
        baseline_path.write_text(json.dumps(inv))

        def run(data, baseline=None, extra=()):
            matrix_path.write_text(json.dumps(data))
            baseline_path.write_text(json.dumps(inv if baseline is None else baseline))
            return subprocess.run([
                "python3", str(HERE / "gate.py"), "check", "--inventory", str(baseline_path),
                "--candidate-inventory", str(baseline_path), "--matrix", str(matrix_path), *extra,
            ], capture_output=True, text=True)

        result = run(matrix)
        self.assertEqual(result.returncode, 0, result.stderr)
        for status in ("pending", "unverified", "unsupported", "needs_decision"):
            changed = copy.deepcopy(matrix)
            changed["entries"][0]["status"] = status
            self.assertNotEqual(run(changed).returncode, 0, status)
        changed = copy.deepcopy(matrix)
        changed["entries"][0]["cases"].pop("explicit")
        self.assertNotEqual(run(changed).returncode, 0)
        changed = copy.deepcopy(matrix)
        changed["runs"]["synthetic"]["tests"]["TestOption"] = "skipped"
        self.assertNotEqual(run(changed).returncode, 0)
        changed = copy.deepcopy(matrix)
        changed["entries"][0]["status"] = "excluded"
        self.assertNotEqual(run(changed, extra=("--allow-exclusions",)).returncode, 0)
        changed["entries"][0]["decision"] = {"reference": "synthetic user request", "quote": "synthetic approval"}
        self.assertNotEqual(run(changed).returncode, 0)
        self.assertEqual(run(changed, extra=("--allow-exclusions",)).returncode, 0)
        drift = copy.deepcopy(inv)
        drift["items"].append({"id": "worker.Options.NewField", "kind": "field", "fingerprint": "new"})
        self.assertNotEqual(run(matrix, baseline=drift).returncode, 0)
        drift = copy.deepcopy(inv)
        drift["snapshots"][0]["digest"] = "changed candidate source"
        self.assertNotEqual(run(matrix, baseline=drift).returncode, 0)
        drift = copy.deepcopy(inv)
        drift["issues"] = ["unresolved alias"]
        self.assertNotEqual(run(matrix, baseline=drift).returncode, 0)
        log.write_text("altered log\n")
        self.assertNotEqual(run(matrix).returncode, 0)


if __name__ == "__main__":
    unittest.main()
