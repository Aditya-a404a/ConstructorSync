# Audit & Alignment Report — ConstructSync Backend & Frontend API Integration

I have conducted a thorough, line-by-line audit of the `ConstructorSync` backend endpoints and the `ConstructorSyncFrontend` application to ensure 100% correctness of request/response schemas, parameters, and CLI commands.

To make everything run seamlessly, I updated the frontend codebase to align directly with the backend schemas and CLI arguments. Here is a verbose summary of what was audited, corrected, and verified.

---

## 1. CLI Commands & Options Alignment

### Audited Discrepancies (Now Corrected):
* **File Path Argument:** The frontend CLI docs previously showed `--file_path data/processed/demo.csv`. In the backend CLI (`cli.py`), this is actually registered as `--file` (and `-f`). Using `--file_path` in the terminal would result in a CLI syntax error.
* **Force Sync Flag:** The frontend previously documented `--force-sync`. The actual CLI flag is `--force` (which sets `force_sync=True` internally).
* **Batch Size Flag:** The frontend CLI docs had `--batch_size` (with an underscore). The argparse parser expects a hyphen: `--batch-size` (or `-b`).
* **Argparse Subcommand List:** The help printout in the documentation showed a `health` subcommand (which is not implemented in CLI subparsers) and was missing the actual `dlq-list` subcommand. 

### Corrected Flag Reference Table:
| CLI Argument | Type | Default | API Field equivalent | Description |
|:---|:---|:---|:---|:---|
| `--source / -s` | String | `"file"` | `"source"` | `"file"`, `"bestbuy"`, `"dummyjson"`, `"kafka"` |
| `--file / -f` | String | `None` | `"file_path"` | CSV/JSONL path (required if source='file') |
| `--category` | String | `None` | `"category"` | API source product category filter |
| `--limit` | Integer | `5000` | `"limit"` | Max fetch limit from live APIs |
| `--target` | String | `None` | `"target"` | Target name (e.g. `"constructor-mock"`) |
| `--health-threshold`| Integer | `None` | `"health_threshold"`| Rating threshold (typically `70`) |
| `--force` | Flag | `False` | `"force_sync"` | Bypass the content-hash fingerprint cache |
| `--batch-size / -b` | Integer | `None` | `"batch_size"` | Chunk size for Constructor batch uploads |
| `--concurrency / -c`| Integer | `None` | `"concurrency"` | Initial asyncio worker thread count |
| `--base-url` | String | `None` | `"base_url"` | Custom Constructor server base URL |
| `--api-key` | String | `None` | `"api_key"` | Custom Constructor client API credential |

*Frontend files edited:* [apiDocsData.ts](file:///Users/adityaarora/ConstructorSyncFrontend/src/lib/apiDocsData.ts) (updated `concept-cli-usage` block).

---

## 2. HTTP REST API Schema & Parameter Alignment

### Audited Discrepancies (Now Corrected):

#### `POST /ingest` (Trigger Ingestion)
* **Discrepancy:** The body parameters list was missing `category`, `target`, `health_threshold`, `base_url`, and `api_key` options.
* **Correction:** Added all 5 missing parameters to the `requestBodyParams` array in `apiDocsData.ts`.

#### `GET /dlq` (Query DLQ Items)
* **Discrepancy:** The schema list was missing the `sku` query parameter, which the backend fully supports.
* **Correction:** Added `{ name: "sku", type: "string" }` to `queryParams` in `apiDocsData.ts`.

#### `POST /dlq/retry` (Reprocess DLQ)
* **Discrepancy:** The documentation showed this endpoint as returning a synchronous `200 OK` with a success count. However, the backend executes this asynchronously as a background task, immediately returning a `202 Accepted` with a `job_id` so that the client can poll progress.
* **Correction:** Updated `apiDocsData.ts` to document `202 Accepted` and the returned `job_id`/`status` json structure.

#### `GET /metrics` (Prometheus Metrics)
* **Discrepancy:** The Prometheus metric example showed `constructsync_items_processed_total{status="sent"}`, but the backend implementation writes status labels as `"success"`, `"failed"`, or `"skipped"`.
* **Correction:** Updated the response example in the docs to match:
  ```text
  constructsync_items_processed_total{status="success"} 10006.0
  constructsync_items_processed_total{status="failed"} 0.0
  constructsync_items_processed_total{status="skipped"} 0.0
  ```

---

## 3. Interactive Docs Explorer Execution Flow

### Audited Discrepancies (Now Corrected):
* **General Mocks:** When clicking "Execute Request" in the API explorer page, the script used placeholder behaviors or called index listing endpoints for singular actions:
  - `get-ingest-job-by-id` called `getJobs()` (fetching the list of all jobs) instead of querying a specific job ID.
  - `get-dlq-by-id` called `getDLQ()` (fetching all items) instead of querying a single item ID.
  - `delete-dlq-by-id` returned a hardcoded mock message `{"status": "success", "message": "Deleted DLQ item 12"}` without actually executing a delete request.

### Code Alignment Fixes:
1. **Added `getDLQItemById`:** Added a fetch handler in `api.ts` to retrieve `/dlq/{item_id}`.
2. **Polished `handleExecuteRequest`:** Updated the execute button controller in `ApiDocsExplorer.tsx` to perform real, dynamic queries:
  - **Specific Job Status:** Queries the active job (`jobs[0]?.job_id`) using `getJobById(jobId)`.
  - **Inspect DLQ Item:** Queries the first DLQ record ID using `getDLQItemById(itemId)`.
  - **Delete DLQ Item:** Submits a real `DELETE /dlq/{item_id}` request to the backend for the first item in the list and triggers a UI list refresh.

*Frontend files edited:*
- [api.ts](file:///Users/adityaarora/ConstructorSyncFrontend/src/lib/api.ts) (Added `getDLQItemById` call)
- [ApiDocsExplorer.tsx](file:///Users/adityaarora/ConstructorSyncFrontend/src/components/ApiDocsExplorer.tsx) (Connected interactive execution cases to correct backend endpoints)

---

## 4. Verification Check

To confirm everything works:
1. **Server Status:** Started both the mock target and engine APIs. The frontend successfully detects the local backend as online (`isLiveBackendAvailable` resolves to `true`).
2. **CLI Check:** Ran `constructsync ingest --source file --file data/processed/demo_products_augmented.csv --concurrency 6` from terminal. Ingestion runs successfully, complying with the argparse parameters.
3. **Execution Button Audit:** Navigated to the REST API explorer in browser, loaded `POST /ingest`, clicked "Execute Request" -> backend schedules background task, frontend captures response with generated `job_id`, progress bar reflects real-time status, and `/metrics` live-scrapes the results.
