# PoC Trino connector for Wrike REST API

# Backlog:
- [x] Schema introspection (`SHOW SCHEMAS`, `SHOW TABLES`, `SHOW COLUMNS`)
- [x] Query tasks, contacts with batches
- [ ] API Pagination support
- [x] JOIN support
- [x] INSERT tasks (`INSERT INTO tasks(title) VALUES('hello')`)
- [x] PUSH-down filter by `id`
- [ ] UPDATE tasks (`UPDATE tasks SET title = 'new title' WHERE id = 'QWERTY'`)
- [ ] DELETE tasks
- [ ] Run Trino with Wrike plugin in embedded mode
- [ ] Read-only tables, columns (`tasks.id`)
