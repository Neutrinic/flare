"""Folds the changelog fragments in changelog.d/ into CHANGELOG.md (#194).

Each pull request adds changelog.d/<issue>.md instead of editing CHANGELOG.md, so no two pull
requests touch the same file and none conflicts with another. A fragment is a Keep a Changelog
heading followed by its entry, written exactly as it will appear:

    ### Fixed
    - **What changed, in a phrase.** Why, and what to do about it ([#176])

Usage:

    python .github/scripts/release_changelog.py --check            # fragments are well formed
    python .github/scripts/release_changelog.py --preview          # print what the release would add
    python .github/scripts/release_changelog.py 1.4.0 2026-10-30   # write the release, delete fragments

The release form puts every fragment, and anything already under [Unreleased], into a new
`## [1.4.0] - 2026-10-30` section grouped by heading, adds the `[#N]` link references the entries
use, moves the [Unreleased] compare link forward and adds the version's own, then deletes the
fragments. Run it in the release pull request and commit the result.
"""
import datetime
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FRAGMENTS = ROOT / "changelog.d"
CHANGELOG = ROOT / "CHANGELOG.md"
REPO = "https://github.com/Neutrinic/flare"
ORDER = ["Added", "Changed", "Deprecated", "Removed", "Fixed", "Security"]


def fragment_files():
    return sorted(p for p in FRAGMENTS.glob("*.md") if p.name != "README.md")


def parse(text, source):
    """Entries by heading. An entry is a `- ` line and the indented lines that continue it."""
    sections, current, errors = {}, None, []
    for n, line in enumerate(text.rstrip("\n").split("\n"), 1):
        heading = re.match(r"^### (\w+)$", line)
        if heading:
            current = heading.group(1)
            if current not in ORDER:
                errors.append(f"{source}:{n}: unknown heading '{current}', use one of {', '.join(ORDER)}")
            sections.setdefault(current, [])
        elif line.startswith("-"):
            if current is None:
                errors.append(f"{source}:{n}: entry before any '### Heading'")
            elif not re.match(r"^- \S", line):
                # An entry's text starts on its own line: "- " then text, not a bare dash.
                errors.append(f"{source}:{n}: empty entry, or text not starting on the '- ' line")
            else:
                sections[current].append(line)
        elif line.startswith("  ") and current and sections[current]:
            sections[current][-1] += "\n" + line
        elif line.strip():
            errors.append(f"{source}:{n}: not a heading, an entry or an indented continuation: {line[:60]}")
    return sections, errors


def load_fragments():
    merged, errors = {}, []
    for path in fragment_files():
        sections, errs = parse(path.read_text(encoding="utf-8"), path.name)
        errors += errs
        if not any(sections.values()):
            errors.append(f"{path.name}: no entries")
        for heading, entries in sections.items():
            merged.setdefault(heading, []).extend(entries)
    return merged, errors


def render(sections):
    blocks = [f"### {h}\n" + "\n".join(sections[h]) for h in ORDER + sorted(set(sections) - set(ORDER))
              if sections.get(h)]
    return "\n\n".join(blocks)


def unreleased_of(text):
    return text.split("## [Unreleased]\n", 1)[1].split("\n## [", 1)[0]


def main(argv):
    fragments, errors = load_fragments()
    if errors:
        sys.exit("\n".join(errors))

    if argv == ["--check"]:
        print(f"{len(fragment_files())} changelog fragment(s), well formed")
        return
    if argv == ["--preview"]:
        # What the release would write: anything already under [Unreleased], then the fragments,
        # through the same reader the release uses.
        existing, errs = parse(unreleased_of(CHANGELOG.read_text(encoding="utf-8")), "CHANGELOG.md [Unreleased]")
        if errs:
            sys.exit("\n".join(errs))
        for heading, entries in fragments.items():
            existing.setdefault(heading, []).extend(entries)
        print(render(existing) or "(nothing to release)")
        return
    if len(argv) != 2 or not re.fullmatch(r"\d+\.\d+\.\d+", argv[0]) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", argv[1]):
        sys.exit(__doc__)
    version, date = argv
    try:
        datetime.date.fromisoformat(date)
    except ValueError:
        sys.exit(f"not a real date: {date}")  # checked before anything is written or deleted

    text = CHANGELOG.read_text(encoding="utf-8")
    head, rest = text.split("## [Unreleased]\n", 1)
    unreleased, tail = rest.split("\n## [", 1)
    tail = "## [" + tail
    existing, errs = parse(unreleased, "CHANGELOG.md [Unreleased]")
    if errs:
        sys.exit("\n".join(errs))
    for heading, entries in fragments.items():
        existing.setdefault(heading, []).extend(entries)
    if not any(existing.values()):
        sys.exit("nothing to release: no fragments and an empty [Unreleased]")

    body = render(existing)
    lines = tail.rstrip("\n").split("\n")
    previous = next(m.group(1) for l in lines for m in [re.match(r"^\[(\d+\.\d+\.\d+)\]: ", l)] if m)
    lines = [f"[Unreleased]: {REPO}/compare/v{version}...HEAD" if l.startswith("[Unreleased]: ") else l for l in lines]
    at = next(i for i, l in enumerate(lines) if l.startswith("[Unreleased]: "))
    lines.insert(at + 1, f"[{version}]: {REPO}/compare/v{previous}...v{version}")

    refs = {int(m) for m in re.findall(r"\[#(\d+)\](?!:)", body)}
    have = {int(m) for m in re.findall(r"^\[#(\d+)\]: ", "\n".join(lines), re.M)}
    issue_refs = [l for l in lines if re.match(r"^\[#\d+\]: ", l)]
    others = [l for l in lines if not re.match(r"^\[#\d+\]: ", l)]
    issue_refs += [f"[#{n}]: {REPO}/issues/{n}" for n in sorted(refs - have)]
    issue_refs.sort(key=lambda l: int(re.match(r"^\[#(\d+)\]", l).group(1)))

    result = (head + "## [Unreleased]\n\n" + f"## [{version}] - {date}\n\n" + body + "\n\n"
              + "\n".join(others).rstrip("\n") + "\n" + "\n".join(issue_refs) + "\n")
    with open(CHANGELOG, "w", encoding="utf-8", newline="\n") as f:
        f.write(result)
    for path in fragment_files():
        path.unlink()
    print(f"released {version}: {sum(len(v) for v in existing.values())} entries, "
          f"{len(refs - have)} new issue link(s); fragments deleted, commit CHANGELOG.md and changelog.d/")


if __name__ == "__main__":
    main(sys.argv[1:])
