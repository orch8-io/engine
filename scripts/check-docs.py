#!/usr/bin/env python3
"""Check repository Markdown links and fenced examples without executing examples.

Uses only Python 3.11+ and bash. Scope comes from Git, so ignored worktrees,
vendor trees, and build products are never traversed.
"""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
import re
import subprocess
import sys
import tomllib
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parent.parent
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
LINK = re.compile(r'!?\[[^\]\n]*\]\((<[^>]+>|[^)\s]+)(?:\s+"[^"]*")?\)')
REFERENCE = re.compile(r"^ {0,3}\[[^\]]+\]:\s*(<[^>]+>|\S+)", re.MULTILINE)


def documents() -> list[Path]:
    result = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
    )
    return sorted({ROOT / name.decode() for name in result.split(b"\0")
                   if name and name.decode().endswith((".md", ".mdx"))})


def split_fences(source: str) -> tuple[str, list[tuple[int, str, str]], int | None]:
    """Blank fenced code in prose while preserving line numbers for diagnostics."""
    prose, examples, body = [], [], []
    opening: tuple[int, str, str] | None = None
    for line_number, line in enumerate(source.splitlines(keepends=True), 1):
        match = FENCE.match(line.rstrip("\r\n"))
        if opening:
            if (match and match[1][0] == opening[1][0]
                    and len(match[1]) >= len(opening[1]) and not match[2].strip()):
                examples.append((opening[0], opening[2], "".join(body)))
                opening, body = None, []
            else:
                body.append(line)
            prose.append("\n")
        elif match:
            opening = (line_number, match[1], match[2].strip())
            prose.append("\n")
        else:
            prose.append(line)
    return "".join(prose), examples, opening[0] if opening else None


def anchors(source: str) -> set[str]:
    prose, _, _ = split_fences(source)
    result = set(re.findall(r'(?:id|name)=["\']([^"\']+)', prose))
    seen: Counter[str] = Counter()
    for heading in re.findall(r"(?m)^ {0,3}#{1,6}\s+(.+?)\s*#*\s*$", prose):
        heading = re.sub(r"<[^>]*>", "", heading)
        # Keep link labels and code text, matching GitHub's heading anchors.
        heading = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", heading)
        slug = re.sub(r"[^\w\- ]", "", heading.lower()).replace(" ", "-")
        occurrence = seen[slug]
        seen[slug] += 1
        result.add(slug + (f"-{occurrence}" if occurrence else ""))
    return result


def main() -> int:
    errors: list[str] = []
    counts: Counter[str] = Counter()
    paths = documents()
    anchor_cache: dict[Path, set[str]] = {}
    for path in paths:
        label = path.relative_to(ROOT)
        source = path.read_text(encoding="utf-8")
        prose, examples, unclosed = split_fences(source)
        if unclosed:
            errors.append(f"{label}:{unclosed}: unclosed code fence")
        for line, language, code in examples:
            counts[language] += 1
            try:
                if language == "json":
                    json.loads(code)
                elif language == "toml":
                    tomllib.loads(code)
                elif language in {"sh", "bash", "shell"}:
                    result = subprocess.run(["bash", "-n"], input=code,
                                            text=True, capture_output=True)
                    if result.returncode:
                        raise ValueError(result.stderr.strip())
            except (ValueError, tomllib.TOMLDecodeError) as error:
                errors.append(f"{label}:{line}: invalid {language}: {error}")
        links = [(m.start(), m[1]) for m in LINK.finditer(prose)]
        links += [(m.start(), m[1]) for m in REFERENCE.finditer(prose)]
        for position, raw in links:
            url = raw.strip("<>")
            parsed = urlsplit(url)
            if parsed.scheme or parsed.netloc:
                continue
            target = (path.parent / unquote(parsed.path)).resolve() if parsed.path else path
            line = prose[:position].count("\n") + 1
            counts["local_links"] += 1
            if not target.exists():
                errors.append(f"{label}:{line}: missing local target: {url}")
            elif parsed.fragment and target.suffix in {".md", ".mdx"}:
                if target not in anchor_cache:
                    anchor_cache[target] = anchors(target.read_text(encoding="utf-8"))
                if unquote(parsed.fragment) not in anchor_cache[target]:
                    errors.append(f"{label}:{line}: missing heading anchor: {url}")
    for error in errors:
        print(error, file=sys.stderr)
    print(f"Checked {len(paths)} Markdown files, {counts['local_links']} local links, "
          f"{counts['json']} JSON, {counts['toml']} TOML, and "
          f"{sum(counts[k] for k in ('sh', 'bash', 'shell'))} shell examples.")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
