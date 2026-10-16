#!/usr/bin/env python3
"""Build the pinned Go fork and SDK tgo driver, or an explicit local checkout."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile

SDK = Path(__file__).resolve().parents[1]
PIN = json.loads((SDK / "toolchain/go-source.json").read_text())


def run(*args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def build(source=None, local=False, output=None):
    if local and source is None:
        raise ValueError("--local requires --source")
    revision = PIN["revision"]
    if not local and not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("go-source.json must pin a full 40-character commit")
    if source is None:
        source = SDK / ".toolchains/source"
        if not (source / ".git").is_dir():
            source.parent.mkdir(parents=True, exist_ok=True)
            subprocess.run(["git", "clone", "--no-checkout", PIN["repository"], str(source)], check=True)
        subprocess.run(["git", "-C", str(source), "fetch", "origin", revision], check=True)
    source = source.resolve()
    if local:
        root = source
        if output is not None and output.resolve() != root:
            raise ValueError("--local builds in place; omit --output")
        revision = run("git", "rev-parse", "HEAD", cwd=source)
    else:
        actual = run("git", "rev-parse", revision + "^{commit}", cwd=source)
        if actual != revision:
            raise ValueError("source revision does not match the toolchain pin")
        root = output.resolve() if output else SDK / ".toolchains" / revision
        if root.exists() and any(root.iterdir()):
            raise ValueError(f"output must be empty: {root}")
        root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryFile() as archive:
            subprocess.run(["git", "archive", revision], cwd=source, stdout=archive, check=True)
            archive.seek(0)
            with tarfile.open(fileobj=archive) as packed:
                packed.extractall(root)
        minor = re.search(r"const Version = (\d+)", (root / "src/internal/goversion/goversion.go").read_text())[1]
        timestamp = run("git", "show", "-s", "--format=%cI", revision, cwd=source)
        (root / "VERSION").write_text(f"go1.{minor}-devel_isolates-{revision[:12]}\ntime {timestamp}\n")
    env = os.environ.copy()
    for key in ("GOROOT", "GOOS", "GOARCH", "GOFLAGS", "GOBIN", "GOEXPERIMENT"):
        env.pop(key, None)
    env.update(GOTOOLCHAIN="local", GOENV="off", GOEXPERIMENT="isolates")
    if "GOROOT_BOOTSTRAP" not in env:
        bootstrap = shutil.which("go")
        if bootstrap is None:
            raise ValueError("set GOROOT_BOOTSTRAP to Go " + PIN["bootstrap"] + " or newer")
        env["GOROOT_BOOTSTRAP"] = run(bootstrap, "env", "GOROOT", env={**env,"GOEXPERIMENT":""})
    subprocess.run(["bash", "make.bash"], cwd=root / "src", env=env, check=True)
    # This package uses only the ordinary standard library and can be built
    # without bootstrapping isolate workflows or downloading SDK dependencies.
    subprocess.run([str(root / "bin/go"), "build", "-trimpath", "-o", str(root / "bin/tgo"), "./cmd/tgo"],
                   cwd=SDK, env={**env, "GOROOT": str(root)}, check=True)
    sdk_revision = os.environ.get("TGO_SDK_REVISION") or run("git", "rev-parse", "HEAD", cwd=SDK)
    if not re.fullmatch(r"[0-9a-f]{40}", sdk_revision):
        raise ValueError("SDK source revision must be a full 40-character commit")
    record = {"go_commit": revision, "sdk_commit": sdk_revision, "local_source": local,
              "experiment": "isolates"}
    (root / "pkg/TGO_BUILD.json").write_text(json.dumps(record, indent=2) + "\n")
    print(root)
    return root


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, help="offline Go fork checkout containing the pinned commit")
    parser.add_argument("--local", action="store_true", help="explicitly build the checkout's current files in place")
    parser.add_argument("--output", type=Path, help="empty output directory for a pinned source snapshot")
    args = parser.parse_args()
    build(args.source, args.local, args.output)


if __name__ == "__main__":
    main()
