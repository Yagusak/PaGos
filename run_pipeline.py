from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pipeline runner for the State Duma parser.")
    parser.add_argument(
        "--mode",
        choices=["full", "stage34", "stage3", "stage4"],
        default="full",
        help="full=stage1+stage2+stage3+stage4; stage34=supervised stage3+stage4 only.",
    )
    parser.add_argument("--output-dir", default="artifacts", help="Artifacts directory for gd_pipeline.cli")
    parser.add_argument("--start-page", type=int, default=1, help="Stage1 start page")
    parser.add_argument("--end-page", type=int, default=24, help="Stage1 end page")
    parser.add_argument("--workers", type=int, default=18, help="Stage2 workers")
    parser.add_argument("--headful", action="store_true", help="Run Playwright with browser UI for stage2")
    parser.add_argument(
        "--direct-stage34",
        action="store_true",
        help="Run stage3/stage4 directly instead of stage34_supervisor.py",
    )
    return parser


def run_command(command: list[str], log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log_fp:
        log_fp.write(f"\n=== {datetime.now().isoformat()} RUN: {' '.join(command)} ===\n")
        log_fp.flush()

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None

        for line in process.stdout:
            print(line, end="")
            log_fp.write(line)

        rc = process.wait()
        log_fp.write(f"=== EXIT CODE: {rc} ===\n")
        log_fp.flush()
    return rc


def cli_cmd(*parts: str) -> list[str]:
    return [sys.executable, "-m", "gd_pipeline.cli", *parts]


def run_stage34_supervisor() -> int:
    return subprocess.run([sys.executable, "stage34_supervisor.py"], check=False).returncode


def main() -> int:
    args = build_parser().parse_args()
    output_dir = Path(args.output_dir)
    log_dir = output_dir / "logs"
    log_path = log_dir / "run_pipeline.log"

    if args.mode in {"full", "stage34"} and not args.direct_stage34 and output_dir.as_posix() != "artifacts":
        print(
            "stage34_supervisor.py currently works with artifacts/ only. "
            "Use --output-dir artifacts or pass --direct-stage34.",
            file=sys.stderr,
        )
        return 2

    if args.mode == "full":
        stage1 = cli_cmd(
            "stage1",
            "--output-dir",
            args.output_dir,
            "--start-page",
            str(args.start_page),
            "--end-page",
            str(args.end_page),
        )
        rc = run_command(stage1, log_path)
        if rc != 0:
            return rc

        stage2 = cli_cmd("stage2", "--output-dir", args.output_dir, "--workers", str(args.workers))
        if args.headful:
            stage2.append("--headful")
        rc = run_command(stage2, log_path)
        if rc != 0:
            return rc

        if args.direct_stage34:
            rc = run_command(cli_cmd("stage3", "--output-dir", args.output_dir), log_path)
            if rc != 0:
                return rc
            return run_command(cli_cmd("stage4", "--output-dir", args.output_dir), log_path)

        return run_stage34_supervisor()

    if args.mode == "stage34":
        if args.direct_stage34:
            rc = run_command(cli_cmd("stage3", "--output-dir", args.output_dir), log_path)
            if rc != 0:
                return rc
            return run_command(cli_cmd("stage4", "--output-dir", args.output_dir), log_path)
        return run_stage34_supervisor()

    if args.mode == "stage3":
        return run_command(cli_cmd("stage3", "--output-dir", args.output_dir), log_path)

    if args.mode == "stage4":
        return run_command(cli_cmd("stage4", "--output-dir", args.output_dir), log_path)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
