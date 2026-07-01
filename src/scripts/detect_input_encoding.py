"""Diagnostica a codificacao mais adequada para arquivos TXT de entrada do CNPJ."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

DEFAULT_ENCODINGS = ["latin1", "cp1252", "utf-8", "utf-8-sig"]

# Padroes comuns de texto quebrado por encoding incorreto.
_MOJIBAKE_PATTERNS = [
    re.compile(r"Ã."),
    re.compile(r"Â."),
    re.compile(r"\ufffd"),
]


def _suspicious_control_chars(text: str) -> int:
    return sum(1 for ch in text if ord(ch) < 32 and ch not in "\n\r\t")


def _accented_chars(text: str) -> int:
    accent_pool = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
    return sum(1 for ch in text if ch in accent_pool)


def _mojibake_hits(text: str) -> int:
    return sum(len(pattern.findall(text)) for pattern in _MOJIBAKE_PATTERNS)


def _read_sample(path: Path, encoding: str, max_lines: int) -> tuple[list[str], int]:
    lines: list[str] = []
    decode_errors = 0

    with path.open("r", encoding=encoding, errors="replace") as fp:
        for _, line in zip(range(max_lines), fp):
            decode_errors += line.count("\ufffd")
            lines.append(line.rstrip("\n\r"))

    return lines, decode_errors


def diagnose_file(path: Path, encodings: list[str], max_lines: int) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []

    for enc in encodings:
        lines, decode_errors = _read_sample(path, enc, max_lines)
        merged = "\n".join(lines)
        mojibake = _mojibake_hits(merged)
        ctrl = _suspicious_control_chars(merged)
        accents = _accented_chars(merged)

        # Menor score e melhor. Penaliza erros e texto suspeito; recompensa acentos validos.
        score = (decode_errors * 100) + (mojibake * 10) + (ctrl * 5) - accents

        results.append(
            {
                "encoding": enc,
                "score": score,
                "decode_errors": decode_errors,
                "mojibake_hits": mojibake,
                "control_chars": ctrl,
                "accented_chars": accents,
                "sample": lines[:3],
            }
        )

    results.sort(key=lambda item: item["score"])
    return results


def iter_target_files(target: Path) -> list[Path]:
    if target.is_file():
        return [target]
    if target.is_dir():
        return sorted([p for p in target.iterdir() if p.is_file() and not p.name.startswith(".")])
    raise FileNotFoundError(f"Caminho nao encontrado: {target}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compara codificacoes e sugere INPUT_FILE_ENCODING para arquivos de entrada.",
    )
    parser.add_argument(
        "target",
        help="Arquivo TXT (ou pasta com arquivos) para diagnostico.",
    )
    parser.add_argument(
        "--encodings",
        default=",".join(DEFAULT_ENCODINGS),
        help="Lista separada por virgula de encodings para teste. Ex.: latin1,cp1252,utf-8",
    )
    parser.add_argument(
        "--max-lines",
        type=int,
        default=200,
        help="Quantidade maxima de linhas para amostra por arquivo.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    target = Path(args.target)
    encodings = [enc.strip() for enc in args.encodings.split(",") if enc.strip()]

    files = iter_target_files(target)
    if not files:
        print("Nenhum arquivo para diagnosticar.")
        return

    suggestions: dict[str, int] = {}

    for file_path in files:
        results = diagnose_file(file_path, encodings, args.max_lines)
        best = results[0]
        suggestions[best["encoding"]] = suggestions.get(best["encoding"], 0) + 1

        print(f"\n[ARQUIVO] {file_path}")
        print(
            "Melhor sugestao: {encoding} | score={score} | erros={decode_errors} | mojibake={mojibake_hits}".format(
                **best
            )
        )

        for rank, item in enumerate(results[:2], start=1):
            print(
                "  {rank}. {encoding} | score={score} | erros={decode_errors} | mojibake={mojibake_hits} | acentos={accented_chars}".format(
                    rank=rank,
                    **item,
                )
            )
            for line in item["sample"]:
                print(f"     > {line[:140]}")

    if suggestions:
        top = sorted(suggestions.items(), key=lambda kv: kv[1], reverse=True)[0]
        print("\n[RECOMENDACAO GERAL]")
        print(f"INPUT_FILE_ENCODING={top[0]} (venceu em {top[1]} arquivo(s))")


if __name__ == "__main__":
    main()
