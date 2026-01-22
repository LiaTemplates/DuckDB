<!--
author:   André Dietrich

version:  0.0.9

email:    LiaScript@web.de

edit:     true

comment:  This template allows you to embed interactive DuckDB queries in LiaScript
          for teaching SQL and data analysis. DuckDB runs entirely in the browser
          using WebAssembly.

@onload
window.duckdbs = window.duckdbs || {}

window.dbdiagramQuery = `WITH
-- 1) Alle Basistabellen
tables AS (
  SELECT table_schema, table_name
  FROM information_schema.tables
  WHERE table_type = 'BASE TABLE'
),

-- 2) Spalten + Datentypen
cols AS (
  SELECT
    c.table_schema,
    c.table_name,
    c.column_name,
    lower(c.data_type) AS data_type,
    c.ordinal_position
  FROM information_schema.columns c
  JOIN tables t USING (table_schema, table_name)
),

-- 3) Primärschlüssel-Spalten markieren
pk_cols AS (
  SELECT k.table_schema, k.table_name, k.column_name
  FROM information_schema.table_constraints tc
  JOIN information_schema.key_column_usage k
    ON tc.constraint_name = k.constraint_name
   AND tc.table_schema    = k.table_schema
   AND tc.table_name      = k.table_name
  WHERE tc.constraint_type = 'PRIMARY KEY'
),

-- 4) Fremdschlüssel: von Spalte → referenzierte Tabelle/Spalte
fk_map AS (
  SELECT
    k.table_schema,
    k.table_name,
    k.column_name,
    ccu.table_schema  AS ref_schema,
    ccu.table_name    AS ref_table,
    ccu.column_name   AS ref_column
  FROM information_schema.table_constraints tc
  JOIN information_schema.key_column_usage k
    ON tc.constraint_name = k.constraint_name
   AND tc.table_schema    = k.table_schema
   AND tc.table_name      = k.table_name
  JOIN information_schema.referential_constraints rc
    ON rc.constraint_name = tc.constraint_name
   AND rc.constraint_schema = tc.table_schema
  JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = rc.unique_constraint_name
   AND ccu.constraint_schema = rc.unique_constraint_schema
  WHERE tc.constraint_type = 'FOREIGN KEY'
),

-- 5) Spaltenzeilen im dbdiagram-Format zusammensetzen
col_lines AS (
  SELECT
    c.table_schema,
    c.table_name,
    c.ordinal_position,
    '  ' || c.column_name || ' ' || c.data_type ||
    CASE WHEN pk.column_name IS NOT NULL THEN ' [pk]' ELSE '' END ||
    CASE
      WHEN fk.ref_table IS NOT NULL
        THEN ' [ref: > ' || fk.ref_table || '.' || fk.ref_column || ']'
      ELSE ''
    END AS line
  FROM cols c
  LEFT JOIN pk_cols pk
    ON pk.table_schema = c.table_schema
   AND pk.table_name   = c.table_name
   AND pk.column_name  = c.column_name
  LEFT JOIN fk_map fk
    ON fk.table_schema = c.table_schema
   AND fk.table_name   = c.table_name
   AND fk.column_name  = c.column_name
)

-- 6) Pro Tabelle zu einem Block aggregieren
SELECT
  'Table ' || t.table_name || ' {\n' ||
  string_agg(cl.line, '\n' ORDER BY cl.ordinal_position) || '\n}'
  AS dbdiagram_block
FROM tables t
JOIN col_lines cl
  ON cl.table_schema = t.table_schema
 AND cl.table_name   = t.table_name
GROUP BY t.table_schema, t.table_name
ORDER BY t.table_schema, t.table_name;`

window.dbdiagram = function (dbml) {
    // Unicode-safe: UTF-8 → base64 → URL-encode
    const base64 = btoa(unescape(encodeURIComponent(dbml)));
    const c = encodeURIComponent(base64);

    return `<iframe
      src="https://dbdiagram.io/embed?c=${c}"
      width="100%"
      height="600px"
      style="border: 0"
      referrerpolicy="no-referrer"
      allowfullscreen
    ></iframe>
    <center><a style="font-size: smaller" target="_blank" href="https://dbdiagram.io/embed?c=${c}">dbdiagram.io</a></center>`;
}

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


window.renderChart = function (rows, opts = {}) {
  const data = Array.isArray(rows) ? rows : [];
  
  if (data.length === 0) {
    return '<div style="padding: 20px; color: #aaa;">No data to display</div>';
  }

  // Helper function to check if value is a date/timestamp
  function isDateLike(value) {
    if (value instanceof Date) return true;
    if (typeof value === 'string') {
      // Check for ISO date format or common date patterns
      const isoDatePattern = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?/;
      return isoDatePattern.test(value);
    }
    return false;
  }

  // Helper function to format date values
  function formatDate(value) {
    if (value instanceof Date) {
      return value.toISOString().slice(0, 10);
    }
    if (typeof value === 'string') {
      // If it looks like a timestamp, format it
      if (value.includes('T')) {
        try {
          return new Date(value).toISOString().slice(0, 10);
        } catch {
          return value;
        }
      }
      return value; // Already in date format
    }
    return value;
  }

  // Helper function to convert values to numbers (but preserve dates for X-axis)
  function toNumber(value, isXAxis = false) {
    if (value === null || value === undefined) return null;
    
    // Handle Uint32Array (DuckDB uses this for certain integer types)
    if (value instanceof Uint32Array) {
      // For single value arrays, extract the number
      if (value.length === 1) return value[0];
      // For multi-value arrays (e.g., 128-bit integers), convert to BigInt then Number
      if (value.length === 4) {
        try {
          const bigIntVal = BigInt(value[0]) + (BigInt(value[1]) << 32n) + 
                           (BigInt(value[2]) << 64n) + (BigInt(value[3]) << 96n);
          return Number(bigIntVal);
        } catch {
          return value[0]; // Fallback to first element
        }
      }
      return value[0]; // Default: return first element
    }
    
    // For X-axis, preserve date/timestamp values
    if (isXAxis && isDateLike(value)) {
      // Keep numeric timestamps as-is for time axis
      if (typeof value === 'number') return value;
      // Format date strings
      return formatDate(value);
    }
    
    // For Y-axis or non-date values, always try to convert to number
    if (typeof value === 'number') return value;
    if (typeof value === 'bigint') return Number(value);
    
    if (typeof value === 'string') {
      // Aggressively remove all quotes and try to parse
      let cleaned = value;
      
      // Try JSON.parse first for escaped strings like "\"281\""
      try {
        const jsonParsed = JSON.parse(cleaned);
        if (typeof jsonParsed === 'number') return jsonParsed;
        if (typeof jsonParsed === 'string') cleaned = jsonParsed;
      } catch {
        // Not JSON, continue with manual cleaning
      }
      
      // Remove all quotes (single and double) and whitespace
      cleaned = cleaned.replace(/["']/g, '').trim();
      
      // Try to parse as number
      const parsed = parseFloat(cleaned);
      // Return parsed number if valid, otherwise return original
      return isNaN(parsed) ? value : parsed;
    }
    
    return value;
  }

  // Extract column names from first row
  const columns = Object.keys(data[0] || {});
  
  if (columns.length < 1) {
    return '<div style="padding: 20px; color: #aaa;">Need at least 1 column for chart</div>';
  }

  // Special case: Single row - transpose data (columns become categories)
  if (data.length === 1) {
    const row = data[0];
    const categories = [];
    const values = [];
    
    for (const col of columns) {
      categories.push(col);
      values.push(toNumber(row[col], false));
    }
    
    const option = {
      title: {
        text: opts.title || '',
        textStyle: { color: '#eee' }
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' }
      },
      grid: {
        left: '3%',
        right: '4%',
        bottom: '3%',
        containLabel: true
      },
      xAxis: {
        type: 'category',
        data: categories,
        nameTextStyle: { color: '#eee' },
        axisLabel: { color: '#aaa', rotate: 45 },
        axisLine: { lineStyle: { color: '#555' } }
      },
      yAxis: {
        type: 'value',
        nameTextStyle: { color: '#eee' },
        axisLabel: { color: '#aaa' },
        axisLine: { lineStyle: { color: '#555' } },
        splitLine: { lineStyle: { color: '#333' } }
      },
      series: [{
        type: 'bar',
        data: values,
        itemStyle: {
          color: '#5470c6'
        }
      }]
    };
    
    return "<lia-chart style='width: 100%; height: 400px; resize: vertical; overflow: auto; display: block; border: 1px solid #555;' option='" + JSON.stringify(option) + "'></lia-chart>";
  }

  // Multi-row case: Normal chart behavior
  if (columns.length < 2) {
    return '<div style="padding: 20px; color: #aaa;">Need at least 2 columns for chart</div>';
  }

  // Assume first column is x-axis, rest are series
  const xColumn = columns[0];
  const yColumns = columns.slice(1);

  // Determine chart type and x-axis type based on first column
  const firstValue = data[0][xColumn];
  const isDateColumn = isDateLike(firstValue);
  const isCategory = typeof firstValue === 'string' && !isDateColumn;
  const chartType = isCategory ? 'bar' : 'line';
  const xAxisType = isDateColumn ? 'time' : 'category';

  // Prepare series data with number conversion
  const series = yColumns.map(col => ({
    name: col,
    type: chartType,
    data: data.map(row => {
      const xValue = toNumber(row[xColumn], true);  // isXAxis = true
      const yValue = toNumber(row[col], false);      // isXAxis = false
      return [xValue, yValue];
    })
  }));

  // Create eCharts option
  const option = {
    title: {
      text: opts.title || '',
      textStyle: { color: '#eee' }
    },
    tooltip: {
      trigger: 'axis'
    },
    legend: {
      data: yColumns,
      textStyle: { color: '#eee' }
    },
    grid: {
      left: '3%',
      right: '4%',
      bottom: '3%',
      containLabel: true
    },
    xAxis: {
      type: xAxisType,
      name: xColumn,
      nameTextStyle: { color: '#eee' },
      axisLabel: { color: '#aaa' },
      axisLine: { lineStyle: { color: '#555' } }
    },
    yAxis: {
      type: 'value',
      nameTextStyle: { color: '#eee' },
      axisLabel: { color: '#aaa' },
      axisLine: { lineStyle: { color: '#555' } },
      splitLine: { lineStyle: { color: '#333' } }
    },
    series: series
  };

  return "<lia-chart style='width: 100%; height: 180px; resize: vertical; overflow: auto; display: block; border: 1px solid #555;' option='" + JSON.stringify(option) + "'></lia-chart>";
}

window.renderTable = function (rows, opts = {}) {
  let { columns, caption, maxRows, schema } = opts;
  const data = Array.isArray(rows) ? rows : [];

  const duckdbTypeMap = {
    0: "NULL",
    1: "BOOL",
    2: "INT8",
    3: "INT16",
    4: "INT32",
    5: "VARCHAR",
    6: "FLOAT",
    7: "DOUBLE",
    8: "DATE",
    9: "TIMESTAMP",
    // ... weitere Typen nach Bedarf ergänzen
  };

  schema = schema && schema.fields
  ? schema.fields.map(f => ({
      name: f.name,
      type: duckdbTypeMap[f.type.typeId] || "UNKNOWN",
      scale: f.type.scale || null,
      precision: f.type.precision || null
    }))
  : undefined;


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
  html += `<thead>`;
  html += `<tr>`;
  // Add empty header for row number
  html += `<th style="${thStyle()}"></th>`;
  for (const c of cols) {
    html += `<th style="${thStyle()}">${escapeHTML(c)}`;
    
    html += `</th>`;
  }
  html += `</tr>`;
  html += `</thead>`;

  // body
    html += `<tbody>`;
    for (let i = 0; i < limit; i++) {
      const row = data[i] ?? {};
      html += `<tr style="${i % 2 === 0 ? trEvenStyle() : trOddStyle()}">`;
      // Add row number as first cell, no header
      html += `<td style="${tdStyle('italic', '#888')}">${i + 1}</td>`;
      for (const c of cols) {
        let cellValue = row[c];
        if (schema && Array.isArray(schema)) {
          const colSchema = schema.find(s => s.name === c);
          if (colSchema && colSchema.type) {
            cellValue = formatCellWithType(cellValue, colSchema);
          } else {
            cellValue = formatCell(cellValue);
          }
        } else {
          cellValue = formatCell(cellValue);
        }
        html += `<td style="${tdStyle()}">${escapeHTML(cellValue)}</td>`;
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

  // Format cell value based on DuckDB type
  function formatCellWithType(v, schema) {

    if (v === null) return 'null';
    if (v === undefined) return 'undefined';
    switch (schema.type.toLowerCase()) {
      case 'bool':
        return v ? 'TRUE' : 'FALSE';
      case 'date': let shorten = true
      case 'timestamp':
        
        if (typeof v === 'string' || v instanceof Date || typeof v === 'number') {
          try {
            let date = new Date(v).toISOString();
            if (shorten) {
              date = date.slice(0,10)
            }
            return date
          } catch {
            return String(v);
          }
        }
        return String(v);
      case 'bigint':
      case 'hugeint':
        // DuckDB returns BigInt as JS BigInt or string
        return typeof v === 'bigint' ? v.toString() : String(v);
      case 'double':
      case 'float':
      case 'real':
      case 'decimal':
        if (schema.scale != null && typeof v === 'number') {
          // Use scale for decimal places
          return v.toLocaleString('en-US', { minimumFractionDigits: schema.scale, maximumFractionDigits: schema.scale });
        }
        if (schema.scale != null && v instanceof Uint32Array && v.length === 4) {
          // Convert Uint32Array decimal to string using scale
          const val = BigInt(v[0]) + (BigInt(v[1]) << 32n) + (BigInt(v[2]) << 64n) + (BigInt(v[3]) << 96n);
          let str = val.toString();
          while (str.length <= schema.scale) str = '0' + str;
          str = str.slice(0, -schema.scale) + '.' + str.slice(-schema.scale);
          if (str.endsWith('.')) str = str.slice(0, -1);
          return str;
        }
        return typeof v === 'number' ? v.toLocaleString('en-US', { maximumFractionDigits: 6 }) : String(v);
      case 'json':
        try { return JSON.stringify(typeof v === 'string' ? JSON.parse(v) : v); } catch { return String(v); }
      default:
        return formatCell(v);
    }
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

await conn.query("PRAGMA explain_output = 'all';");

// Run a query
const statements = `@input`
  .split(';')
  .map(s => s.trim())
  .filter(s => s.length > 0);

for (const stmt of statements) {
  try {
    if (statements.length > 1) {
      console.debug(stmt);
    }

    if (stmt.toLowerCase().startsWith("erdiagram")) {
      let diagram = await conn.query(window.dbdiagramQuery);
      diagram = diagram.toArray().map(r => r["dbdiagram_block"]).join("\n\n");
      diagram = window.dbdiagram(diagram);
      console.html(diagram)
    } else if (stmt.toLowerCase().startsWith("chart")) {
      // Extract query after CHART keyword
      const query = stmt.substring(6).trim();
      let result = await conn.query(query);
      let array = result.toArray();
      console.html(window.renderChart(array));
    } else {
      let result = await conn.query(stmt);

      let array = result.toArray();
      if (array.length > 0 && array[0]["explain_value"]) {
        console.debug(array[0]["explain_value"]);
      } else {
        console.html(window.renderTable(array, {schema : result.schema }));
      }
    }
  } catch (error) {
    console.error("Error executing statement:", stmt, error.message);
  }
}
 conn.close()
  send.lia("LIA: stop")

}, 100)

"LIA: wait"
</script>
@end

@DuckDB.terminal
<script>
setTimeout(async () => {
let db = await window.duckdbinit("@0")
const conn = await db.connect();

// Run initial query/queries
const statements = `@input`
  .split(';')
  .map(s => s.trim())
  .filter(s => s.length > 0);

for (const stmt of statements) {
  try {
    if (statements.length > 1) {
      console.debug(stmt);
    }

    if (stmt.toLowerCase().startsWith("erdiagram")) {
      let diagram = await conn.query(window.dbdiagramQuery);
      diagram = diagram.toArray().map(r => r["dbdiagram_block"]).join("\n\n");
      diagram = window.dbdiagram(diagram);
      console.html(diagram)
    } else if (stmt.toLowerCase().startsWith("chart")) {
      // Extract query after CHART keyword
      const query = stmt.substring(6).trim();
      let result = await conn.query(query);
      let array = result.toArray();
      console.html(window.renderChart(array));
    } else {
      let result = await conn.query(stmt);
      console.html(window.renderTable(result.toArray(), {schema: result.schema}));
    }
  } catch (error) {
    console.error(error.message)
  }
}

send.handle("input", async (input) => {
    // Run a query
const statements = input
  .split(';')
  .map(s => s.trim())
  .filter(s => s.length > 0);

for (const stmt of statements) {
  try {
    if (stmt.toLowerCase().startsWith("erdiagram")) {
      let diagram = await conn.query(window.dbdiagramQuery);
      diagram = diagram.toArray().map(r => r["dbdiagram_block"]).join("\n\n");
      diagram = window.dbdiagram(diagram);
      console.html(diagram)
    } else if (stmt.toLowerCase().startsWith("chart")) {
      // Extract query after CHART keyword
      const query = stmt.substring(6).trim();
      let result = await conn.query(query);
      let array = result.toArray();
      console.html(window.renderChart(array));
    } else {
      let result = await conn.query(stmt);

      let array = result.toArray();
      if (array.length > 0 && array[0]["explain_value"]) {
        console.debug(array[0]["explain_value"]);
      } else {
        console.html(window.renderTable(array, {schema: result.schema}));
        
      }
    }
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

   or the current version 0.0.9 via:

   `import: https://raw.githubusercontent.com/LiaTemplates/DuckDB/0.0.9/README.md`

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
@DuckDB.terminal(demo)

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

## Advanced Features (& `CHART`)

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
CHART
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