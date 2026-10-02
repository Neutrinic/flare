# Changelog fragments

Each pull request adds one file here, named after its issue, such as `176.md`, and does not edit
`CHANGELOG.md`. Two pull requests never touch the same file, so neither conflicts with the other.

A fragment is a [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) heading followed by its
entry, written exactly as it will appear in `CHANGELOG.md`:

```markdown
### Fixed
- **`FLARE_ENABLED=false` turns off the executors' task metrics too.** Spans honoured the kill
  switch, but executors still recorded `flare.task.*` ([#176])
```

- Headings: `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`. A fragment can hold
  more than one.
- Continue a long entry on lines indented by two spaces.
- Reference issues as `[#176]`; the link is added at release.

`python .github/scripts/release_changelog.py --check` validates the fragments, and CI runs it.
The release pull request runs the script with the version and date, which folds every fragment
into `CHANGELOG.md` and deletes them.
