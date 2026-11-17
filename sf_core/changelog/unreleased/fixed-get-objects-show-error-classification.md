---
category: bugfix
---

- Fixed GetObjects suppressing SHOW syntax and access errors based on SQLSTATE. Only Snowflake error codes 2003 and 2043 now produce an empty metadata scope.
