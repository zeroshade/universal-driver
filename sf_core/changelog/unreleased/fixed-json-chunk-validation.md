---
category: bugfix
---

- Fixed JSON chunks silently dropping truncated rows or ignoring garbage between rows. Invalid UTF-8 values now use replacement characters, and downloaded JSON chunks reject mismatched row counts.
