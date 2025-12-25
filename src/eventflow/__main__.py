import argparse
import json
from pathlib import Path
from .experiments import demo, evaluate
from .processor import Processor
from .io import load_config, load_inputs


def main():
    parser = argparse.ArgumentParser(description="Durable event-time windows and local replay experiments")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("demo", "evaluate"):
        command = commands.add_parser(name)
        command.add_argument("--output", type=Path)
    command = commands.add_parser("run", help="Consume a bounded JSONL source into a persistent local checkpoint")
    command.add_argument("--input", type=Path, required=True)
    command.add_argument("--database", type=Path, required=True)
    command.add_argument("--config", type=Path)
    command.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.command == "run":
        try:
            config = load_config(args.config)
            records = load_inputs(args.input)
            args.database.parent.mkdir(parents=True, exist_ok=True)
            with Processor(str(args.database), config) as processor:
                counts = {"committed": 0, "duplicate": 0}
                for record in records:
                    counts[processor.apply(record)["status"]] += 1
                report = {"inputs": len(records), "outcomes": counts, "state": processor.snapshot()}
        except (ValueError, RuntimeError) as exc:
            parser.error(str(exc))
    else:
        report = demo() if args.command == "demo" else evaluate()
    text = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    else:
        print(text, end="")
    if (args.command == "demo" and not report["passed"]) or (
        args.command == "evaluate" and report["passed"] != report["total"]
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
