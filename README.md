# Wrike SQL
Proof of concept of full-scale SQL over [Wrike REST API](https://developers.wrike.com).
This prototype is based on the [Trino](https://trino.io) project and actually holds a custom implementation of Trino's connector.
This project is a domain-specific "query engine" with just ~1k LoC which contains a bridge between REST endpoints/models and SQL API.

# Why
1. Compatible "SQL API" which can be exposed along with REST, GraphQL, gRPC, etc
2. Interchange format can be easily integrated with any database tool via the Trino JDBC driver (DataGrip, DbVisualizer, etc)
3. Native integration with data analytics tools due to standard SQL types and schema introspection (Tableau, Metabase, etc)
4. Data mesh with cross-domain data joins.
   It is possible to transfer, combine and aggregate data from REST API response with any data sources like S3, BigQuery, or Snowflake 
   using a big variety of [connectors](https://github.com/trinodb/trino/tree/master/plugin)

# How to run
1. Install JDK 17+
2. Create an app and permanent token [here](https://www.wrike.com/frontend/apps/index.html#api)
3. `./mvnw -Dtoken=${TOKEN} -Dport=${PORT} -Dtest=TestEmbedded#run test`
4. Connect to `jdbc:trino://127.0.0.1:${PORT}`

# Examples
<details>
   <summary>Schema inspection</summary>
   <img src="https://github.com/alekkol/wrike-sql/raw/main/examples/create_connection.gif" alt="Schema inspection">
</details>

<details>
   <summary>Data manipulation using SQL</summary>
   <img src="https://github.com/alekkol/wrike-sql/raw/main/examples/data_manipulation.gif" alt="Schema inspection">
   <ul>
      <li>
         <p>Basic insert</p>
         <code>
         INSERT INTO wrike.rest.tasks(title) VALUES('Buy a car');
         </code>
      </li>
      <li>
         <p>Select with filter and sorting</p>
         <code>
         SELECT * FROM wrike.rest.tasks WHERE createddate > NOW() - INTERVAL '5' MINUTE ORDER BY createddate DESC;
         </code>   
      </li>
      <li>
         <p>Insert with FK</p>
         <code>
         INSERT INTO wrike.rest.comments(taskid, text) VALUES('', 'Not today');
         </code>
      </li>
      <li>
         <p>Update from previous and new states</p>
         <code>
         UPDATE wrike.rest.tasks SET title = title || '?', status = 'Cancelled' WHERE id = '';
         </code>
      </li>
      <li>
         <p>Delete and subquery</p>
         <code>
         DELETE FROM wrike.rest.comments
         WHERE authorid IN (SELECT c.id FROM wrike.rest.contacts c WHERE c.firstname like 'Александр%');
         </code>
      </li>
      <li>
         <p>Joining and grouping</p>
         <code>
         SELECT c.firstname, COUNT(*) assigned
         FROM wrike.rest.tasks t
         JOIN wrike.rest.contacts c ON contains(t.responsibleids, c.id)
         GROUP BY c.firstname;
         </code>
      </li>
  </ul>
</details>

<details>
   <summary>ETL: REST -> PostgreSQL</summary>
   <img src="https://github.com/alekkol/wrike-sql/raw/main/examples/etl.gif" alt="Schema inspection">
   
```
   SELECT task.title,
          task.createddate,
          con_author.firstname author,
          listagg(con_responsible.firstname, ', ') WITHIN GROUP (ORDER BY task.title) AS responsibles,
          last_value(com.text) OVER (PARTITION BY task.title ORDER BY com.createddate DESC) last_comment
   FROM wrike.rest.tasks task
   JOIN wrike.rest.contacts con_author ON contains(task.authorids, con_author.id)  
   LEFT JOIN wrike.rest.contacts con_responsible ON contains(task.responsibleids, con_responsible.id)
   LEFT JOIN wrike.rest.comments com ON com.taskid = task.id
   GROUP BY task.title, task.createddate, con_author.firstname, com.createddate, com.text;
```
</details>

# Scope of prototype
- [x] Schema introspection (`SHOW SCHEMAS`, `SHOW TABLES`, `SHOW COLUMNS`)
- [x] Query tasks, contacts with batches
- [x] API Pagination support
- [x] JOIN support
- [x] INSERT tasks (`INSERT INTO tasks(title) VALUES('hello')`)
- [x] PUSH-down filter by one or multiple `id` column values
- [x] DELETE tasks (`DELETE FROM tasks WHERE id = 'QWERTY'`)
- [x] UPDATE tasks (`UPDATE tasks SET title = 'new title' WHERE id = 'QWERTY'`)
- [x] UPDATE from previous state (`UPDATE tasks SET title = title || '!' WHERE id = 'QWERTY'`)
- [x] Run Trino with Wrike plugin in embedded mode
- [ ] Read-only tables, columns (`contacts`, ``tasks.id`, `tasks.permalink`)