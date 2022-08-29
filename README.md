# Wrike SQL
Proof of concept of full-scale SQL over [Wrike REST API](https://developers.wrike.com). This prototype is based on the [Trino](https://trino.io) project and actually holds a custom implementation of Trino's connector.

# How to run
1. Create an app and permanent token [here](https://www.wrike.com/frontend/apps/index.html#api)
2. 

# But why
1. Build a bridge between REST endpoints/models and relational SQL API.
   This interchange format can be easily integrated with any database tool via the Trino JDBC driver (DataGrip, DbVisualizer, etc)
2. Native integration with data analytics tools due to standard SQL types and schema introspection (Tableau, Metabase, etc)
3. Data mesh with cross-domain data joins.
   It is possible to combine and aggregate data from REST API response with any sources like S3, BigQuery, or Snowflake using a big variety of [connectors](https://github.com/trinodb/trino/tree/master/plugin)

# Examples

# Scope of prototype
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