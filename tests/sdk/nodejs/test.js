/**
 * Node.js SDK Compatibility Test for BigQuery Emulator
 *
 * Prerequisites:
 *   npm install @google-cloud/bigquery
 *
 * Usage:
 *   1. Start the emulator: bigquery-emulator --project=test-project --port=9050
 *   2. Run: BIGQUERY_EMULATOR_HOST=localhost:9050 node test.js
 */

const { BigQuery } = require("@google-cloud/bigquery");

const PROJECT_ID = "test-project";
const DATASET_ID = "nodejs_sdk_test";
const TABLE_ID = "users";
const API_ENDPOINT =
  process.env.BIGQUERY_EMULATOR_HOST || "localhost:9050";

const bigquery = new BigQuery({
  apiEndpoint: API_ENDPOINT,
  projectId: PROJECT_ID,
});

let passed = 0;
let failed = 0;

function pass(name) {
  passed++;
  console.log(`[PASS] ${name}`);
}

function fail(name, err) {
  failed++;
  console.error(`[FAIL] ${name}: ${err.message || err}`);
}

async function testCreateDataset() {
  const name = "Create dataset";
  try {
    const [dataset] = await bigquery.createDataset(DATASET_ID);
    if (dataset.id !== DATASET_ID) {
      throw new Error(`Expected dataset id '${DATASET_ID}', got '${dataset.id}'`);
    }
    pass(name);
  } catch (err) {
    fail(name, err);
  }
}

async function testCreateTable() {
  const name = "Create table";
  try {
    const schema = [
      { name: "id", type: "INT64", mode: "REQUIRED" },
      { name: "name", type: "STRING", mode: "NULLABLE" },
      { name: "email", type: "STRING", mode: "NULLABLE" },
      { name: "score", type: "FLOAT64", mode: "NULLABLE" },
    ];

    const dataset = bigquery.dataset(DATASET_ID);
    const [table] = await dataset.createTable(TABLE_ID, { schema });
    if (table.id !== TABLE_ID) {
      throw new Error(`Expected table id '${TABLE_ID}', got '${table.id}'`);
    }
    pass(name);
  } catch (err) {
    fail(name, err);
  }
}

async function testInsertRows() {
  const name = "Insert rows";
  try {
    const table = bigquery.dataset(DATASET_ID).table(TABLE_ID);
    const rows = [
      { id: 1, name: "Alice", email: "alice@example.com", score: 95.5 },
      { id: 2, name: "Bob", email: "bob@example.com", score: 87.3 },
      { id: 3, name: "Charlie", email: "charlie@example.com", score: 92.1 },
    ];

    await table.insert(rows);
    pass(name);
  } catch (err) {
    fail(name, err);
  }
}

async function testQuery() {
  const name = "Query rows";
  try {
    const query = `SELECT name, score FROM \`${PROJECT_ID}.${DATASET_ID}.${TABLE_ID}\` ORDER BY score DESC`;
    const [rows] = await bigquery.query({ query, useLegacySql: false });

    if (!Array.isArray(rows)) {
      throw new Error("Expected rows to be an array");
    }
    if (rows.length !== 3) {
      throw new Error(`Expected 3 rows, got ${rows.length}`);
    }
    // First row should be Alice (highest score)
    if (rows[0].name !== "Alice") {
      throw new Error(`Expected first row name 'Alice', got '${rows[0].name}'`);
    }
    pass(name);
  } catch (err) {
    fail(name, err);
  }
}

async function testListDatasets() {
  const name = "List datasets";
  try {
    const [datasets] = await bigquery.getDatasets();

    if (!Array.isArray(datasets)) {
      throw new Error("Expected datasets to be an array");
    }
    const ids = datasets.map((ds) => ds.id);
    if (!ids.includes(DATASET_ID)) {
      throw new Error(`Expected to find '${DATASET_ID}' in datasets: ${ids}`);
    }
    pass(name);
  } catch (err) {
    fail(name, err);
  }
}

async function testListTables() {
  const name = "List tables";
  try {
    const dataset = bigquery.dataset(DATASET_ID);
    const [tables] = await dataset.getTables();

    if (!Array.isArray(tables)) {
      throw new Error("Expected tables to be an array");
    }
    const ids = tables.map((t) => t.id);
    if (!ids.includes(TABLE_ID)) {
      throw new Error(`Expected to find '${TABLE_ID}' in tables: ${ids}`);
    }
    pass(name);
  } catch (err) {
    fail(name, err);
  }
}

async function testErrorHandling() {
  const name = "Error handling";
  try {
    const query = "SELECT * FROM `nonexistent_dataset.nonexistent_table`";
    await bigquery.query({ query, useLegacySql: false });
    throw new Error("Expected query to fail but it succeeded");
  } catch (err) {
    // We expect an error here - that's the test passing
    if (err.message === "Expected query to fail but it succeeded") {
      fail(name, err);
    } else {
      pass(name);
    }
  }
}

// ---------------------------------------------------------------------------
// New tests for SDK compatibility: projects, etag, pagination
// ---------------------------------------------------------------------------

async function testProjectList() {
  const name = "Project list endpoint";
  try {
    // Use raw HTTP request since the Node.js SDK doesn't expose project list directly
    const http = require("http");
    const url = `http://${API_ENDPOINT}/bigquery/v2/projects`;

    const body = await new Promise((resolve, reject) => {
      http.get(url, (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => resolve(JSON.parse(data)));
        res.on("error", reject);
      }).on("error", reject);
    });

    if (body.kind !== "bigquery#projectList") {
      throw new Error(`Expected kind 'bigquery#projectList', got '${body.kind}'`);
    }
    if (!Array.isArray(body.projects) || body.projects.length === 0) {
      throw new Error("Expected non-empty projects array");
    }
    const proj = body.projects[0];
    if (!proj.projectReference || !proj.projectReference.projectId) {
      throw new Error("Missing projectReference.projectId in project entry");
    }
    pass(name);
  } catch (err) {
    fail(name, err);
  }
}

async function testDatasetEtag() {
  const name = "Dataset response has etag";
  try {
    const dataset = bigquery.dataset(DATASET_ID);
    const [metadata] = await dataset.getMetadata();

    // The SDK exposes etag from the response
    if (!metadata.etag || typeof metadata.etag !== "string" || metadata.etag === "") {
      throw new Error(`Expected non-empty etag string, got '${metadata.etag}'`);
    }
    pass(name);
  } catch (err) {
    fail(name, err);
  }
}

async function testTableEtag() {
  const name = "Table response has etag";
  try {
    const table = bigquery.dataset(DATASET_ID).table(TABLE_ID);
    const [metadata] = await table.getMetadata();

    if (!metadata.etag || typeof metadata.etag !== "string" || metadata.etag === "") {
      throw new Error(`Expected non-empty etag string, got '${metadata.etag}'`);
    }
    pass(name);
  } catch (err) {
    fail(name, err);
  }
}

async function testTablePagination() {
  const name = "Table list pagination with pageToken";
  try {
    // Create additional tables for pagination testing
    const dataset = bigquery.dataset(DATASET_ID);
    const schema = [
      { name: "id", type: "INT64", mode: "REQUIRED" },
    ];

    // Create extra tables (we already have TABLE_ID from earlier)
    await dataset.createTable("pag_table_b", { schema });
    await dataset.createTable("pag_table_c", { schema });

    // List tables with maxResults=1 to force pagination
    const [tables, nextQuery] = await dataset.getTables({ maxResults: 1 });

    if (!Array.isArray(tables) || tables.length === 0) {
      throw new Error("Expected at least 1 table in first page");
    }
    if (tables.length > 1) {
      throw new Error(`Expected maxResults=1 to return 1 table, got ${tables.length}`);
    }

    // nextQuery should contain a pageToken for fetching the next page
    if (!nextQuery || !nextQuery.pageToken) {
      throw new Error("Expected nextQuery with pageToken for pagination");
    }

    // Fetch next page using the pageToken
    const [tables2] = await dataset.getTables(nextQuery);
    if (!Array.isArray(tables2) || tables2.length === 0) {
      throw new Error("Expected tables in second page");
    }

    pass(name);
  } catch (err) {
    fail(name, err);
  }
}

async function main() {
  console.log(`BigQuery Emulator Node.js SDK Test`);
  console.log(`Endpoint: ${API_ENDPOINT}`);
  console.log(`Project:  ${PROJECT_ID}`);
  console.log("---");

  await testCreateDataset();
  await testCreateTable();
  await testInsertRows();
  await testQuery();
  await testListDatasets();
  await testListTables();
  await testErrorHandling();

  // New compatibility tests
  await testProjectList();
  await testDatasetEtag();
  await testTableEtag();
  await testTablePagination();

  console.log("---");
  console.log(`Results: ${passed} passed, ${failed} failed`);

  if (failed > 0) {
    process.exit(1);
  } else {
    console.log("All tests passed!");
  }
}

main().catch((err) => {
  console.error("Unhandled error:", err);
  process.exit(1);
});
