# PoC Trino connector for Wrike REST API

# Backlog:
- [x] Schema introspection (`SHOW SCHEMAS`, `SHOW TABLES`, `SHOW COLUMNS`)
- [x] Query tasks, contacts with batches
- [x] API Pagination support
- [x] JOIN support
- [x] INSERT tasks (`INSERT INTO tasks(title) VALUES('hello')`)
- [x] PUSH-down filter by `id`
- [x] DELETE tasks (`DELETE FROM tasks WHERE id = 'QWERTY'`)
- [x] UPDATE tasks (`UPDATE tasks SET title = 'new title' WHERE id = 'QWERTY'`)
- [ ] UPDATE from previous state (`UPDATE tasks SET title = title || '!' WHERE id = 'QWERTY'`)
- [ ] Run Trino with Wrike plugin in embedded mode
- [ ] Read-only tables, columns (`contacts`, ``tasks.id`, `tasks.permalink`)
