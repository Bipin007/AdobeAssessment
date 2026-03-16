"""One arg: input file. Writes [date]_SearchKeywordPerformance.tab to cwd."""
import sys
from datetime import date
from pathlib import Path

from local.processor import SearchKeywordProcessor


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python -m local.cli <input_file>", file=sys.stderr)
        return 1

    input_path = Path(sys.argv[1])
    if not input_path.exists():
        print(f"File not found: {input_path}", file=sys.stderr)
        return 1

    processor = SearchKeywordProcessor()
    rows = processor.process_tsv_file(str(input_path))
    out_path = Path.cwd() / processor.output_filename(date.today())
    processor.write_output(rows, str(out_path))
    print(f"Wrote {len(rows)} rows to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
