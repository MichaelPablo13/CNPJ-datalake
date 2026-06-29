"""Profiling simples de layouts CNPJ para arquivos com ou sem extensão."""

from __future__ import annotations

import argparse
from pathlib import Path


def count_fields(line: str, delimiter: str = ";") -> int:
    return len(line.rstrip("\n\r").split(delimiter))


def profile_folder(folder: Path, sample_lines: int = 100) -> dict:
    files = [p for p in folder.iterdir() if p.is_file()]
    report = {
        "folder": str(folder),
        "file_count": len(files),
        "samples": [],
    }

    for path in files[:3]:
        min_fields = None
        max_fields = None
        distinct = set()
        first_line = ""

        with path.open("r", encoding="latin1", errors="ignore") as f:
            for idx, line in enumerate(f):
                if idx == 0:
                    first_line = line.strip()
                if idx >= sample_lines:
                    break
                fields = count_fields(line)
                distinct.add(fields)
                min_fields = fields if min_fields is None else min(min_fields, fields)
                max_fields = fields if max_fields is None else max(max_fields, fields)

        report["samples"].append(
            {
                "name": path.name,
                "size_bytes": path.stat().st_size,
                "extension": path.suffix or "sem_extensao",
                "min_fields": min_fields,
                "max_fields": max_fields,
                "distinct_fields": sorted(distinct),
                "first_line": first_line,
            }
        )

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Profiler de arquivos CNPJ")
    parser.add_argument("folders", nargs="+", help="Pastas para analisar")
    args = parser.parse_args()

    for item in args.folders:
        report = profile_folder(Path(item))
        print(f"\n[PASTA] {report['folder']} | arquivos={report['file_count']}")
        for sample in report["samples"]:
            print(
                " - {name} ({size_bytes} bytes, {extension}) min/max={min_fields}/{max_fields} distintos={distinct_fields}".format(
                    **sample
                )
            )


if __name__ == "__main__":
    main()
