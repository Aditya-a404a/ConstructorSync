# ConstructorSync — Agent Rules

## Git Workflow
- **Never run `git commit` directly.** At the end of a response, suggest the commit message and let the user commit manually.
- **Never run `git push` directly.** Always let the user handle pushing.
- Only run `git add` if the user explicitly asks to stage files.
