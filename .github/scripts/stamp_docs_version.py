"""Sets the Flare version in the documentation's snippets, the only place the docs name it.

    python .github/scripts/stamp_docs_version.py v1.4.0

The docs workflow runs this on a release tag before building, so the published site always names
the release it was built from. Run it in a release PR too, so the repository matches.
"""
import re
import sys
from pathlib import Path

tag = sys.argv[1]
if not re.fullmatch(r"v\d+\.\d+\.\d+", tag):
    sys.exit(f"not a release tag: {tag}")
version = tag[1:]

COORD = r"flare-spark-\d-\d_2\.1[23]"
patterns = [
    # Maven Central and GitHub Release URLs: .../flare-spark-3-5_2.12/1.3.0/flare-spark-3-5_2.12-1.3.0.jar
    (re.compile(rf"({COORD})/\d+\.\d+\.\d+/({COORD})-\d+\.\d+\.\d+\.jar"), rf"\g<1>/{version}/\g<2>-{version}.jar"),
    (re.compile(rf"/download/v\d+\.\d+\.\d+/({COORD})-\d+\.\d+\.\d+\.jar"), rf"/download/v{version}/\g<1>-{version}.jar"),
    # Maven coordinates: io.github.neutrinic:flare-spark-3-5_2.12:1.3.0
    (re.compile(rf"(io\.github\.neutrinic:{COORD}):\d+\.\d+\.\d+"), rf"\g<1>:{version}"),
]

changed = 0
for path in sorted(Path(__file__).resolve().parents[2].joinpath("docs", "snippets").iterdir()):
    text = path.read_text(encoding="utf-8")
    new = text
    for pattern, replacement in patterns:
        new = pattern.sub(replacement, new)
    if new != text:
        path.write_text(new, encoding="utf-8")
        changed += 1
print(f"stamped {version} into {changed} snippet file(s)")
