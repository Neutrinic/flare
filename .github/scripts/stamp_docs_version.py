"""Sets the Flare version in the documentation's snippets, the only place the docs name it.

    python .github/scripts/stamp_docs_version.py v1.4.0

The docs workflow runs this on a release tag before building, so the published site always names
the release it was built from. Run it in a release PR too, so the repository matches.

It fails rather than succeed quietly: if the snippets' format drifts so that no version is found,
or any Flare version other than the new one is left afterwards, the site would name a stale release.
"""
import re
import sys
from pathlib import Path

tag = sys.argv[1] if len(sys.argv) > 1 else ""
if not re.fullmatch(r"v\d+\.\d+\.\d+", tag):
    sys.exit(f"not a release tag: {tag!r}")
version = tag[1:]

SNIPPETS = Path(__file__).resolve().parents[2].joinpath("docs", "snippets")
COORD = r"flare-spark-\d-\d_2\.1[23]"
V = r"\d+\.\d+\.\d+"

replacements = [
    # Maven Central and GitHub Release URLs: .../flare-spark-3-5_2.12/1.3.0/flare-spark-3-5_2.12-1.3.0.jar
    (re.compile(rf"({COORD})/{V}/({COORD})-{V}\.jar"), rf"\g<1>/{version}/\g<2>-{version}.jar"),
    (re.compile(rf"/download/v{V}/({COORD})-{V}\.jar"), rf"/download/v{version}/\g<1>-{version}.jar"),
    # Maven coordinates: io.github.neutrinic:flare-spark-3-5_2.12:1.3.0
    (re.compile(rf"(io\.github\.neutrinic:{COORD}):{V}"), rf"\g<1>:{version}"),
]
# Every Flare version the snippets name, in any of the forms above.
any_version = re.compile(
    rf"{COORD}/({V})/|{COORD}-({V})\.jar|/download/v({V})/|io\.github\.neutrinic:{COORD}:({V})")

changed = 0
for path in sorted(SNIPPETS.iterdir()):
    text = path.read_text(encoding="utf-8")
    new = text
    for pattern, replacement in replacements:
        new = pattern.sub(replacement, new)
    if new != text:
        with open(path, "w", encoding="utf-8", newline="\n") as fh:  # LF on every platform
            fh.write(new)
        changed += 1

found = [v for path in sorted(SNIPPETS.iterdir())
         for m in any_version.finditer(path.read_text(encoding="utf-8"))
         for v in m.groups() if v]
stale = sorted({v for v in found if v != version})
if not found:
    sys.exit("no Flare version found in docs/snippets: the patterns no longer match the snippets")
if stale:
    sys.exit(f"docs/snippets still name {', '.join(stale)} after stamping {version}")
print(f"stamped {version} into {changed} snippet file(s); all {len(found)} Flare versions now read {version}")
