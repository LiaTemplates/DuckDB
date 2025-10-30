<!--
author:   André Dietrich

version:  0.0.2

email:    LiaScript@web.de

edit:     true

comment:  This template allows you to embed interactive DuckDB queries in LiaScript
          for teaching SQL and data analysis. DuckDB runs entirely in the browser
          using WebAssembly.

@onload
window.duckdbs = window.duckdbs || {}

if (!window.duckdb) {
  import("https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm")
  .then( async (module) => {
    window.duckdb = module

    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);

    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
    );

    window.duckdbinit = async function(dbName) {
      if(window.duckdbs[name]) {
        return window.duckdbs[name]
      }

      const worker = new Worker(worker_url);
      const logger = new duckdb.ConsoleLogger();
      const db = new duckdb.AsyncDuckDB(logger, worker);

      // Load the WASM file
      await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

      window.duckdbs[name] = db

      return window.duckdbs[name]
    }
  })
  .catch(error => {
    console.warn("error loading duckdb", error.message)
  })
}


window.renderTable = function (rows, opts = {}) {
  const { columns, caption, maxRows } = opts;
  const data = Array.isArray(rows) ? rows : [];

  if (data.length === 0) {
    return `<table style="${baseTableStyle()}"><caption style="${captionStyle()}">${
      escapeHTML(caption || 'No data')
    }</caption></table>`;
  }

  const cols = columns && columns.length
    ? columns.slice()
    : Array.from(
        data.reduce((set, obj) => {
          Object.keys(obj || {}).forEach(k => set.add(k));
          return set;
        }, new Set())
      );

  const limit =
    typeof maxRows === 'number'
      ? Math.min(data.length, Math.max(0, maxRows))
      : data.length;

  let html = `<table style="${baseTableStyle()}">`;
  if (caption)
    html += `<caption style="${captionStyle()}">${escapeHTML(caption)}</caption>`;

  // header
    html += `<thead><tr>`;
    // Add empty header for row number
    html += `<th style="${thStyle()}"></th>`;
    for (const c of cols) {
      html += `<th style="${thStyle()}">${escapeHTML(c)}</th>`;
    }
    html += `</tr></thead>`;

  // body
    html += `<tbody>`;
    for (let i = 0; i < limit; i++) {
      const row = data[i] ?? {};
      html += `<tr style="${i % 2 === 0 ? trEvenStyle() : trOddStyle()}">`;
      // Add row number as first cell, no header
      html += `<td style="${tdStyle('italic', '#888')}">${i + 1}</td>`;
      for (const c of cols) {
        html += `<td style="${tdStyle()}">${escapeHTML(formatCell(row[c]))}</td>`;
      }
      html += `</tr>`;
    }
    html += `</tbody>`;

  // footer if truncated
    if (limit < data.length) {
      html += `<tfoot><tr><td colspan="${cols.length + 1}" style="${tdStyle(
        'italic',
        '#aaa'
      )}">Showing ${limit} of ${data.length} rows</td></tr></tfoot>`;
    }

  html += `</table>`;
  return html;

  // helpers
  function escapeHTML(str) {
    return String(str ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }
  function formatCell(v) {
    if (v === null) return 'null';
    if (v === undefined) return 'undefined';
    if (v instanceof Date) return v.toISOString();
    if (typeof v === 'object') {
      try { return JSON.stringify(v); } catch { return '[Object]'; }
    }
    return String(v);
  }

  // dark theme styles
  function baseTableStyle() {
    return `
      border-collapse:collapse;
      width:100%;
      font:13px/1.4 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
      border:1px solid #444;
      margin:.5em 0;
      color:#eee;
      background:#1e1e1e;
    `.trim();
  }
  function captionStyle() {
    return `text-align:left;padding:.4em 0;font-weight:600;color:#fff;`;
  }
  function thStyle() {
    return `
      background:#2b2b2b;
      border:1px solid #555;
      padding:.4em .6em;
      text-align:left;
      position:sticky;
      top:0;
      color:#fff;
    `.trim();
  }
  function tdStyle(fontStyle='normal', color='inherit') {
    return `
      border:1px solid #333;
      padding:.4em .6em;
      vertical-align:top;
      font-style:${fontStyle};
      color:${color};
    `.trim();
  }
  function trEvenStyle() {
    return `background:#262626;`;
  }
  function trOddStyle() {
    return `background:#1e1e1e;`;
  }
}
@end


@DuckDB.eval
<script>
setTimeout(async () => {
let db = await window.duckdbinit("@0")
const conn = await db.connect();

// Run a query
const statements = `@input`
  .split(';')
  .map(s => s.trim())
  .filter(s => s.length > 0);

for (const stmt of statements) {
  try {
    const result = await conn.query(stmt);
    console.html(window.renderTable(result.toArray()));
  } catch (error) {
    console.error("Error executing statement:", stmt, error.message);
  }
}
 conn.close()
  send.lia("")

}, 100)

"LIA: wait"
</script>
@end

@DuckDB.terminal
<script>
setTimeout(async () => {
let db = await window.duckdbinit("@0")
const conn = await db.connect();

// Run a query
conn.query(`@input`)
.then((result) => {
  console.html(window.renderTable(result.toArray()));
})
.catch((error) => {
  console.error(error.message)
})

send.handle("input", async (input) => {
    // Run a query
const statements = input
  .split(';')
  .map(s => s.trim())
  .filter(s => s.length > 0);

for (const stmt of statements) {
  try {
    const result = await conn.query(stmt);
    console.html(window.renderTable(result.toArray()));
  } catch (error) {
    console.error("Error executing statement:", stmt, error.message);
  }
}

})

send.handle("stop", () => {
    conn.close()
})

}, 100)

"LIA: terminal"
</script>
@end

@DuckDB.js
<script>
setTimeout(async () => {
let db = await window.duckdbinit("@0")

const conn = await db.connect();

@input

send.lia("")
}, 100)

"LIA: wait"
</script>
@end

-->

# DuckDB

    --{{0}}--
DuckDB is a fast in-process analytical database management system. This LiaScript
template allows you to embed interactive DuckDB queries directly in your
educational materials, enabling students to experiment with SQL and data analysis in
the browser using DuckDB-Wasm.

For more information about DuckDB, visit the [DuckDB website](https://duckdb.org).

- __Try it on LiaScript:__

  https://liascript.github.io/course/?https://raw.githubusercontent.com/LiaTemplates/DuckDB/main/README.md

- __See the project on Github:__

  https://github.com/LiaTemplates/DuckDB

- __Experiment in the LiveEditor:__

  https://liascript.github.io/LiveEditor/?/show/file/https://raw.githubusercontent.com/LiaTemplates/DuckDB/main/README.md

    --{{1}}--
Like with other LiaScript templates, there are three ways to integrate DuckDB, but
the easiest way is to copy the import statement into your project.

                            {{1}}
1. Load the latest macros via (this might cause breaking changes)

   `import: https://raw.githubusercontent.com/LiaTemplates/DuckDB/main/README.md`

   or the current version 0.0.2 via:

   `import: https://raw.githubusercontent.com/LiaTemplates/DuckDB/0.0.2/README.md`

2. __Copy the definitions into your Project__

3. Clone this repository on GitHub

## `@DuckDB.eval`

    --{{0}}--
This is the most common way to run DuckDB queries in LiaScript. It executes SQL
queries and displays the results in a nicely formatted table. The macro requires a
database name parameter (which allows you to have multiple independent databases) and
executes the SQL code block.

``` SQL
SELECT 'Hello, DuckDB!' AS greeting, 42 AS answer;
```
@DuckDB.eval(demo)

    --{{1}}--
You can create tables, insert data, and perform complex analytical queries:

    {{1}}
``` SQL
-- Create a table with sample data
CREATE TABLE weather AS
SELECT * FROM (VALUES
    ('San Francisco', 46, 50, 0.25, DATE '1994-11-27'),
    ('San Francisco', 43, 57, 0.0, DATE '1994-11-29'),
    ('Hayward', 37, 54, NULL, DATE '1994-11-29')
) AS t(city, temp_lo, temp_hi, prcp, date);

-- Query the data
SELECT city, (temp_hi + temp_lo) / 2 AS temp_avg, date
FROM weather
ORDER BY temp_avg DESC;
```
@DuckDB.eval(demo)

    --{{2}}--
DuckDB excels at analytical queries with aggregations and window functions:

    {{2}}
``` SQL
SELECT 
    city,
    COUNT(*) AS num_readings,
    AVG(temp_hi) AS avg_high,
    MAX(temp_hi) AS max_high,
    MIN(temp_lo) AS min_low
FROM weather
GROUP BY city
ORDER BY city;
```
@DuckDB.eval(demo)

## `@DuckDB.terminal`

    --{{0}}--
This macro creates an interactive terminal where users can execute multiple queries
in succession. Unlike `@DuckDB.eval`, which executes once, the terminal mode allows
continuous interaction with the database. Users can type queries and see results
immediately.

``` SQL
-- Initial query to set up the database
CREATE TABLE products AS
SELECT * FROM (VALUES
    (1, 'Laptop', 999.99, 'Electronics'),
    (2, 'Mouse', 29.99, 'Electronics'),
    (3, 'Desk', 299.99, 'Furniture'),
    (4, 'Chair', 199.99, 'Furniture')
) AS t(id, name, price, category);

-- Try running these queries in the terminal below:
-- SELECT * FROM products;
-- SELECT category, AVG(price) AS avg_price FROM products GROUP BY category;
-- SELECT * FROM products WHERE price > 100 ORDER BY price;
```
@DuckDB.terminal(shop)

    --{{1}}--
The terminal maintains the database state, so you can build upon previous queries.
This is excellent for teaching incremental database operations and allowing students
to explore data interactively.

## `@DuckDB.js`

    --{{0}}--
For advanced use cases, this macro allows you to write custom JavaScript code that
interacts with the DuckDB connection object. This is useful when you need more
control over query execution, want to process results programmatically, or integrate
with other JavaScript libraries.

``` js
// Create a table with sample data
await conn.query(`
  CREATE TABLE sales AS
  SELECT * FROM (VALUES
      ('2024-01', 'Product A', 1500),
      ('2024-01', 'Product B', 2300),
      ('2024-02', 'Product A', 1800),
      ('2024-02', 'Product B', 2100),
      ('2024-03', 'Product A', 2200),
      ('2024-03', 'Product B', 2500)
  ) AS t(month, product, revenue)
`);

// Query and process results
const result = await conn.query(`
  SELECT 
    product,
    SUM(revenue)::INTEGER AS total_revenue,
    AVG(revenue)::INTEGER AS avg_revenue,
    COUNT(*)::INTEGER AS num_months
  FROM sales
  GROUP BY product
  ORDER BY total_revenue DESC
`);

// Access the results
const data = result.toArray();
console.log("Sales Analysis:");

// Display results in a custom format
let output = '<div style="padding: 10px; background: #1e1e1e; color: #eee; font-family: monospace;">';
output += '<h3 style="color: #fff; margin-top: 0;">Sales Summary</h3>';

for (const row of data) {
  output += `<div style="margin: 8px 0; padding: 8px; background: #2b2b2b; border-left: 3px solid #4a9eff;">`;
  output += `<strong style="color: #4a9eff;">${row.product}</strong><br/>`;
  output += `Total Revenue: $${row.total_revenue.toLocaleString()}<br/>`;
  output += `Average: $${row.avg_revenue.toLocaleString()}<br/>`;
  output += `Months: ${row.num_months}`;
  output += `</div>`;
}

output += '</div>';
console.html(output);
```
@DuckDB.js(analytics)

    --{{1}}--
The connection object (`conn`) is automatically provided and connected. You have
full access to the DuckDB JavaScript API, allowing you to execute complex workflows,
handle errors, and integrate with other browser APIs.

    {{2}}
> **Note on BigInt values:** DuckDB often returns numeric aggregations as BigInt
> values, which cannot be serialized to JSON. To avoid errors, cast large numbers to
> INTEGER using `::INTEGER` in your SQL queries, or handle BigInt values explicitly
> in your JavaScript code using `.toString()` or `Number()` conversion.

## Working with Files

    --{{0}}--
DuckDB can read various file formats directly from URLs, including CSV, JSON, and
Parquet files. This makes it perfect for teaching data analysis with real-world
datasets.

``` SQL
-- Read a CSV file from a URL
SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/duckdb/duckdb-web/main/data/weather.csv')
LIMIT 10;
```
@DuckDB.eval(files)

## Advanced Features

    --{{0}}--
DuckDB supports a rich set of SQL features including window functions, CTEs
(Common Table Expressions), and complex aggregations:

``` SQL
-- Create sample time-series data
CREATE TABLE daily_sales AS
SELECT 
    DATE '2024-01-01' + INTERVAL (day) DAY AS sale_date,
    50 + (random() * 50)::INTEGER AS sales,
    'Store ' || ((day % 3) + 1) AS store
FROM range(30) AS t(day);

-- Calculate moving averages with window functions
SELECT 
    sale_date,
    store,
    sales,
    AVG(sales) OVER (
        PARTITION BY store 
        ORDER BY sale_date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_3day
FROM daily_sales
ORDER BY store, sale_date
LIMIT 15;
```
@DuckDB.eval(advanced)

    --{{1}}--
You can also use CTEs for complex queries:

    {{1}}
``` SQL
-- Using Common Table Expressions (CTEs)
WITH monthly_stats AS (
    SELECT 
        store,
        DATE_TRUNC('week', sale_date) AS week,
        SUM(sales) AS weekly_total,
        AVG(sales) AS weekly_avg
    FROM daily_sales
    GROUP BY store, DATE_TRUNC('week', sale_date)
)
SELECT 
    store,
    COUNT(*) AS num_weeks,
    AVG(weekly_total) AS avg_weekly_sales,
    MAX(weekly_total) AS best_week,
    MIN(weekly_total) AS worst_week
FROM monthly_stats
GROUP BY store
ORDER BY avg_weekly_sales DESC;
```
@DuckDB.eval(advanced)

## Database Isolation

    --{{0}}--
Each macro call can use a different database name (the parameter in parentheses).
This allows you to have multiple independent databases in the same document:

``` SQL
CREATE TABLE users AS
SELECT * FROM (VALUES
    (1, 'Alice', 'alice@example.com'),
    (2, 'Bob', 'bob@example.com')
) AS t(id, name, email);

SELECT * FROM users;
```
@DuckDB.eval(db1)

``` SQL
-- This is a completely separate database
CREATE TABLE orders AS
SELECT * FROM (VALUES
    (101, 'Order A', 150.00),
    (102, 'Order B', 200.00)
) AS t(order_id, description, amount);

SELECT * FROM orders;
```
@DuckDB.eval(db2)

    --{{1}}--
The database name parameter ensures isolation between different examples and
exercises in your course material.

## Use Cases

    --{{0}}--
This template is perfect for:

- **Teaching SQL**: Interactive SQL tutorials where students can modify and run queries
- **Data Analysis Courses**: Demonstrate analytical techniques with real datasets
- **Database Concepts**: Show table creation, indexing, transactions, and query optimization
- **Business Intelligence**: Teach aggregations, window functions, and reporting queries
- **Data Science**: Pre-process and explore datasets before visualization
- **Interactive Examples**: Allow readers to experiment with queries in documentation

## Implementation

The LiaScript implementation of DuckDB is based on DuckDB-Wasm version 1.29.0,
which runs entirely in the browser using WebAssembly. The implementation includes
custom table rendering with a dark theme and support for multiple concurrent database
instances.

### Technical Details

- **DuckDB-Wasm**: Uses the official `@duckdb/duckdb-wasm` package
- **Web Workers**: Queries run in a separate worker thread for better performance
- **Table Rendering**: Custom HTML table renderer with dark theme styling
- **Multiple Databases**: Supports concurrent isolated database instances
- **Result Display**: Automatic formatting of query results with row limiting

## Examples

For more examples and detailed usage, see the [DuckDB documentation](https://duckdb.org/docs/)