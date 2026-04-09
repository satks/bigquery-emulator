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
