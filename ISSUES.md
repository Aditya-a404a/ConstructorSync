# ConstructSync — Issues Roadmap

> Every feature is an issue. Every issue explains *what* we're building and *why* it matters.
> Work through milestones in order. Each milestone produces a working, demoable state.

---

## Milestone 1: Foundation

These issues set up the project skeleton. Nothing fancy — just the scaffolding that everything else builds on.

---

### Issue #1 — Project Scaffolding

**What:** Set up the Python project structure with FastAPI, pyproject.toml, Docker, and directory layout.

**Why:** We need a clean foundation before writing any feature code. Using `pyproject.toml` (not `requirements.txt`) because it's the modern Python standard. Using FastAPI because it's async-native (our entire pipeline is I/O-bound) and it matches Constructor's internal Python stack.

**Deliverables:**
- [ ] `pyproject.toml` with all dependencies (fastapi, uvicorn, aiohttp, bleach, polars, pytest, hypothesis)
- [ ] Directory structure: `src/constructsync/`, `tests/`, `data/`, `scripts/`
- [ ] `Dockerfile` and `docker-compose.yml` (even if services are placeholder)
- [ ] `.env.example` with documented config variables
- [ ] Basic FastAPI app that starts and returns `{"status": "ok"}` on `/health`

**Acceptance Criteria:** `docker-compose up` starts the app. `curl localhost:8000/health` returns 200.

---

### Issue #2 — Constructor API Mock Server

**What:** Build a standalone FastAPI server that faithfully replicates Constructor's catalog ingestion API behavior, based on their public documentation.

**Why:** Constructor has no developer sandbox or free tier. We need a target API to test against. But this isn't a throwaway mock — it replicates their *documented* constraints precisely, so swapping it for a real Constructor endpoint requires only changing `BASE_URL` and `API_KEY` in `.env`.

**Behavior to replicate (from Constructor docs):**
- [ ] `POST /v2/items` — accepts batch of up to 1,000 items per call
- [ ] Queue limit of 1,000 tasks — return `429 Too Many Requests` when exceeded
- [ ] `X-RateLimit-Tasks-Remaining` header on every response (decrements realistically)
- [ ] Item size validation: reject items where data field > 200KB
- [ ] Average data field size across batch must be ≤ 30KB
- [ ] Accept JSON and JSONL formats
- [ ] Store ingested items in SQLite (for verification in tests and demos)
- [ ] **Chaos mode** (configurable): random 500s, connection timeouts, slow responses
- [ ] API key validation via `Authorization` header

**Acceptance Criteria:** Can POST 5,000 items and observe realistic rate-limiting behavior. Chaos mode produces recoverable failures.

---

### Issue #3 — Dataset Downloader & Preprocessor

**What:** Create a script that downloads real e-commerce datasets and prepares them for ingestion.

**Why:** We're using real data, not generated fakes. But datasets are large (some > 1GB) and can't live in the repo. This script downloads them on demand and converts them to the format our pipeline expects.

**Data sources:**
- [ ] [Amazon Product Dataset 2020](https://www.kaggle.com/datasets/promptcloud/amazon-product-dataset-2020) — 690K products with real data quality issues
- [ ] [Amazon UK 2023](https://www.kaggle.com/datasets/ahmedshabanelshazly/amazon-uk-products-dataset-2023) — 2.2M products for scale testing
- [ ] [Best Buy Developer API](https://developer.bestbuy.com/) — live product data for real-time integration demo
- [ ] [Dirty E-Commerce Data](https://www.kaggle.com/datasets/kashishrastogi/dirty-e-commerce-data) — 80K+ products specifically designed to be messy

**Deliverables:**
- [ ] `scripts/download_datasets.py` — downloads datasets to `data/raw/`
- [ ] `scripts/prepare_datasets.py` — converts to Constructor-compatible schema (maps `About Product` → `description`, `Selling Price` → `price`, etc.)
- [ ] Supports `--sample N` flag for quick testing with a subset
- [ ] Documents data provenance (source, license, download date) in `data/README.md`

**Acceptance Criteria:** `python scripts/download_datasets.py --dataset amazon-2020 --sample 10000` downloads and prepares 10K real products in under 60 seconds.

---

## Milestone 2: Core Pipeline

The engine that actually moves data. After this milestone, you have a working ingestion pipeline.

---

### Issue #4 — Chunked Async Ingestion Engine

**What:** Build the core ingestion engine that reads a large catalog file, splits it into batches of 1,000 items, and sends them concurrently to the Constructor API using async workers.

**Why:** Constructor's batch API accepts up to 1,000 items per call. A catalog of 690K items means 690 API calls. Doing them sequentially would take ~2 hours at 200ms/call. With 8 concurrent workers, it takes ~15 minutes. With adaptive concurrency (Issue #6), even less.

**Implementation details:**
- [ ] Streaming CSV/JSONL reader using Polars (don't load entire file into memory)
- [ ] Configurable batch size (default: 1,000 to match Constructor's limit)
- [ ] Async worker pool using `asyncio.Semaphore` for bounded concurrency
- [ ] Each worker: take batch → process through pipeline stages → POST to API
- [ ] Progress bar with real-time stats (items/sec, batches remaining, error count)

**Key design decision:** The ingestion engine is a *pipeline* — each item flows through: validate → sanitize → health-score → send. The engine orchestrates this flow; the individual stages are separate modules (Issues #5, #7, #9).

**Acceptance Criteria:** Can ingest 100K items from a CSV file into the mock API in under 2 minutes. No item is lost or duplicated. Progress is visible in real-time.

---

### Issue #5 — Targeted Field Sanitization Layer

**What:** Build a sanitization module that strips dangerous HTML/JS from product descriptions while preserving legitimate formatting — and crucially, **only sanitizes text fields, never identifiers or numerics.**

**Why this is harder than it sounds:**

A naive sanitizer (strip all HTML) would:
1. **Destroy legitimate formatting** — `<b>`, `<ul>`, `<br>` are intentional in product descriptions
2. **Corrupt identifiers** — a SKU like `SKU-<ABC>-123` would become `SKU--123`
3. **Lose data** — `"Response time < 2ms"` would have `< 2ms` treated as a malformed tag and stripped

Our sanitizer must be **targeted** (only process configured text fields) and **context-aware** (entity-encode bare `<` instead of stripping it).

**Field targeting configuration:**
```yaml
sanitize:
  text_fields: [description, features, about_product]   # HTML sanitization
  id_fields: [sku, item_id, group_id]                    # type validation only
  numeric_fields: [price, rating, review_count]           # type validation only
  url_fields: [image_url, product_url]                   # URL validation only
```

**Sanitization rules:**
- [ ] **Allow:** `<b>`, `<i>`, `<strong>`, `<em>`, `<ul>`, `<ol>`, `<li>`, `<br>`, `<p>`, `<span>`
- [ ] **Strip (with content):** `<script>`, `<iframe>`, `<object>`, `<embed>`, `<applet>`
- [ ] **Strip (keep content):** `<div>`, `<font>`, `<marquee>`, `<blink>`, `<center>`
- [ ] **Strip all event handlers:** `onclick`, `onerror`, `onload`, `onmouseover`, etc.
- [ ] **Strip dangerous attributes:** `style` (with `expression()`), `href="javascript:..."`
- [ ] **Entity-encode:** bare `<` and `>` that aren't part of tags → `&lt;` and `&gt;`
- [ ] **Normalize:** double-encoded entities (`&amp;lt;` → `&lt;`)

**Acceptance Criteria:** Passes the OWASP XSS test suite (Issue #13) with zero payloads getting through. Processes the full Amazon 2020 dataset with zero false positives (no legitimate data destroyed).

---

### Issue #6 — Adaptive Concurrency Controller (AIMD)

**What:** Instead of a fixed number of workers, implement Additive-Increase/Multiplicative-Decrease (AIMD) — the same algorithm TCP uses for congestion control — to dynamically adjust how many concurrent API calls we make.

**Why:**
- Fixed concurrency of 4 workers? You're leaving throughput on the table during off-peak hours.
- Fixed concurrency of 16 workers? You'll get hammered with 429s during peak hours.
- AIMD automatically finds the sweet spot and adapts to changing conditions.

**Algorithm:**
```
initial_concurrency = 4
max_concurrency = 32
min_concurrency = 1

on_success (200):
    concurrency = min(concurrency + 1, max_concurrency)

on_rate_limit (429):
    concurrency = max(concurrency / 2, min_concurrency)
    wait(backoff_with_jitter)

on_server_error (500, 502, 503):
    concurrency = max(concurrency - 1, min_concurrency)
    wait(exponential_backoff)
```

**Why this is interesting:** This is a direct application of distributed systems theory (TCP Reno congestion control) to API rate limiting. It's the kind of algorithmic choice that signals depth beyond "I used `asyncio`."

**Deliverables:**
- [ ] `ConcurrencyController` class with AIMD logic
- [ ] Integrates with the ingestion engine's `asyncio.Semaphore`
- [ ] Logs concurrency changes: `"429 received → reducing concurrency from 12 to 6"`
- [ ] Exposes current concurrency as a Prometheus metric (Issue #14)

**Acceptance Criteria:** When running against the mock API with chaos mode enabled, the controller demonstrably ramps up during calm periods and backs off during 429 storms. Total throughput is ≥ 30% higher than fixed concurrency of 4.

---

### Issue #7 — Dead-Letter Queue + Sync Report Generator

**What:** Build a DLQ for items that fail after all retries, and a report generator that summarizes every sync run.

**Why:** In an enterprise pipeline, a silently dropped SKU = lost revenue. The DLQ ensures nothing disappears. The sync report gives operators a one-glance summary of what happened and what needs attention.

**DLQ requirements:**
- [ ] Store failed items with full error context (reason, timestamp, retry count, original data, sanitized data)
- [ ] Queryable by reason, timestamp, SKU
- [ ] `retry` command to re-process failed items
- [ ] Backed by SQLite (local) — no external dependencies
- [ ] Configurable max retries before DLQ (default: 3)

**Sync report requirements:**
- [ ] Total items processed, succeeded, failed
- [ ] Sanitization stats (items sanitized, tags stripped, entities encoded)
- [ ] Health score distribution (avg, min, max, items below threshold)
- [ ] API stats (calls made, 429s handled, peak concurrency, total time, throughput)
- [ ] Output as both console table and JSON file (for automation)

**Acceptance Criteria:** After ingesting 100K items with 0.5% intentional failures, every failed item is in the DLQ with correct error context. Sync report matches actual counts (verified against mock API's SQLite store).

---

## Milestone 3: Data Intelligence

These features separate ConstructSync from "just a data mover" — they make it a quality assurance tool.

---

### Issue #8 — Best Buy API Live Data Connector

**What:** Add a second ingestion mode: pull live product data from the [Best Buy Developer API](https://developer.bestbuy.com/), transform it to Constructor's schema, and push it through the pipeline.

**Why:** This is the single most impressive demo feature. It shows the pipeline isn't just a file processor — it's a real-time data integration tool. An engineering reviewer watches your Loom video and sees live data flowing from a real retailer API through sanitization into Constructor's format.

**Implementation:**
- [ ] Best Buy API client (free API key from developer.bestbuy.com)
- [ ] Schema transformer: Best Buy fields → Constructor fields mapping
- [ ] Pagination handler (Best Buy API returns pages of 100)
- [ ] Configurable category filter (e.g., "laptops", "tvs", "appliances")
- [ ] Rate-limit aware (Best Buy has its own rate limits — 5 calls/sec)

**Demo command:**
```bash
constructsync ingest --source bestbuy --category "laptops" --limit 5000 --target constructor-mock
```

**Acceptance Criteria:** Can pull 5,000 real Best Buy products, transform them, sanitize them, and push them to the mock API in a single command. The sync report shows real product names and categories.

---

### Issue #9 — Catalog Health Scoring Engine

**What:** Score each product SKU on a 0–100 "searchability" scale based on field completeness, description quality, and data richness.

**Why:** This ties directly to Constructor's mission. Constructor's AI ranks products based on available data — a product with a rich description, multiple images, complete attributes, and accurate pricing will rank higher than a sparse listing. Health scoring quantifies this, giving catalog teams a concrete metric to improve.

**Scoring rules:**
```
Base Score: 100 (perfect)

Deductions:
  -40  Missing price (critical — Constructor may reject)
  -25  Missing/empty description
  -15  Missing image URL
  -10  Missing category
  -10  Description too short (< 50 chars)
   -5  Missing brand
   -5  Description quality issues (ALL CAPS, excessive special chars)
   -3  Missing secondary attributes (color, size, material)

Bonuses:
  +5   Has multiple images
  +3   Has structured attributes (key-value pairs)
  +2   Description length > 200 chars
```

**Deliverables:**
- [ ] `HealthScorer` class with configurable rules
- [ ] Per-item score included in sync report
- [ ] Aggregate stats: distribution histogram, items below threshold
- [ ] Threshold configuration: items below X score get flagged (not blocked)

**Acceptance Criteria:** Running against the real Amazon 2020 dataset produces meaningful score distribution (not all 100s, not all 0s). Scores correlate visibly with actual data quality when spot-checked.

---

### Issue #10 — Idempotent Delta Sync (Content Hashing)

**What:** Hash each item's content; on repeat syncs, skip items that haven't changed since the last sync.

**Why:** Real enterprise catalogs re-sync daily (or more frequently). A catalog of 500K items where only 2% changed daily means 490K unnecessary API calls. Content hashing eliminates this waste — reducing API calls, reindex latency, and compute cost.

**Implementation:**
- [ ] SHA-256 hash of each item's sanitized payload
- [ ] Hash store: SQLite table mapping `(sku, hash, last_synced_at)`
- [ ] On sync: compute hash → check store → skip if unchanged
- [ ] Force-sync flag: `--force` to bypass hash check
- [ ] Report: "Skipped 485,000 unchanged items. Synced 15,000 modified items."

**Why SHA-256 (not MD5):** We're hashing potentially untrusted data. MD5 has known collision attacks. SHA-256 is the right choice for a security-conscious pipeline.

**Acceptance Criteria:** Second run of the same dataset completes in < 10 seconds (vs. ~4 minutes for the first run). Only modified items are sent to the API.

---

## Milestone 4: Advanced Features

Stretch goals that add depth. Each is independently valuable — skip any if time is tight.

---

### Issue #11 — Kafka Event-Driven Ingestion Path

**What:** Add a third ingestion mode: consume product change events from a Kafka topic and sync them incrementally.

**Why:** Most real PIM (Product Information Management) systems don't do full catalog dumps — they emit change events (`product.created`, `product.updated`, `product.deleted`). Supporting Kafka shows you understand how catalog sync actually works in production, not just toy batch jobs.

**Implementation:**
- [ ] Kafka consumer using `aiokafka`
- [ ] Event schema: `{"event": "product.updated", "sku": "...", "data": {...}}`
- [ ] Micro-batching: collect events for 5 seconds, then process as a batch
- [ ] Docker Compose includes Kafka + Zookeeper (or KRaft mode)
- [ ] Producer script for demo: simulates a PIM emitting change events

**Acceptance Criteria:** Start the consumer, run the producer script emitting 1,000 events, observe them flowing through the pipeline in real-time.

---

### Issue #12 — LLM Attribute Backfill (Stretch)

**What:** For items that fail health checks due to missing structured attributes (color, material, category), use an LLM to infer them from the raw description — flagged as `ai_generated: true`.

**Why:** Constructor is actively building AI Shopping Agents. Showing you can respect a human-in-the-loop boundary around AI-generated catalog data — enriching without overriding — is a mature, senior-level design choice.

**Implementation:**
- [ ] Configurable LLM provider (OpenAI, local Ollama)
- [ ] Prompt template: extract attributes from description text
- [ ] All AI-generated fields marked with `"source": "ai_inferred", "confidence": 0.87`
- [ ] Below-threshold confidence → flagged for human review, not auto-applied
- [ ] Cost tracking: log tokens used and estimated cost per item

**Acceptance Criteria:** Run against 100 items with missing categories. LLM correctly infers category for ≥ 80% of items (verified manually). All inferred fields are marked as AI-generated.

---

### Issue #13 — OWASP Security Test Suite + Fuzz Testing

**What:** Build a comprehensive security test suite for the sanitizer using OWASP XSS vectors and Hypothesis property-based testing.

**Why:** This is what proves the sanitizer *actually works* — not "I tested 5 examples," but "I tested 2,000+ known attack vectors and auto-generated 10,000 adversarial strings." This is the kind of testing that security teams at Constructor would respect.

**Test categories:**
- [ ] **OWASP vectors:** All payloads from the [XSS Filter Evasion Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/XSS_Filter_Evasion_Cheat_Sheet.html)
  - Script tags (basic, encoded, nested)
  - Event handlers (onclick, onerror, onload, onfocus)
  - Protocol handlers (javascript:, data:, vbscript:)
  - CSS expression attacks
  - SVG-based XSS
  - Encoding tricks (hex, unicode, base64, HTML entities)
- [ ] **False positive tests:** Run sanitizer against entire real Amazon dataset, assert zero legitimate data is destroyed
- [ ] **Hypothesis fuzz testing:**
  - Property: sanitizer output never contains `<script`, `onerror=`, `javascript:`, `<iframe`
  - Property: sanitizer output is valid HTML (no unclosed tags introduced)
  - Property: sanitizer is idempotent (sanitize(sanitize(x)) == sanitize(x))
  - Generate: random Unicode strings, nested tag combinations, encoded payloads

**Acceptance Criteria:** `pytest tests/security/ -v` passes with 100% of OWASP vectors blocked and 0% false positives on real data. Hypothesis runs 10,000+ generated cases without failure.

---

### Issue #14 — Prometheus Metrics Endpoint

**What:** Expose a `/metrics` endpoint with Prometheus-format metrics for pipeline observability.

**Why:** Shows you think about running this in production, not just demoing it once. An engineering reviewer sees `/metrics` and knows you've operated real services.

**Metrics to expose:**
- [ ] `constructsync_items_processed_total` (counter, labels: status=success|failed|skipped)
- [ ] `constructsync_items_sanitized_total` (counter)
- [ ] `constructsync_api_requests_total` (counter, labels: status_code=200|429|500)
- [ ] `constructsync_api_request_duration_seconds` (histogram)
- [ ] `constructsync_batch_size` (gauge)
- [ ] `constructsync_current_concurrency` (gauge)
- [ ] `constructsync_dlq_depth` (gauge)
- [ ] `constructsync_health_score` (histogram)

**Acceptance Criteria:** While a sync is running, `curl localhost:8000/metrics` returns valid Prometheus text format with live-updating values.

---

## Milestone 5: Polish & Demo

Make it presentable. An impressive project with a bad README is invisible.

---

### Issue #15 — Architecture Diagram + README Polish

**What:** Create a professional architecture diagram (Excalidraw or Mermaid) and finalize the README with complete setup instructions, benchmarks, and design rationale.

**Why:** The README is the first thing a Constructor engineer reads. If it's sloppy, they won't clone the repo. If it's clear, detailed, and shows engineering depth in the *writing*, they'll assume the code is equally thoughtful.

**Deliverables:**
- [ ] Architecture diagram (embedded in README)
- [ ] Benchmark results table (throughput at various concurrency levels)
- [ ] Design decisions section (why AIMD, why SHA-256, why Polars over Pandas)
- [ ] "Limitations & Future Work" section (shows self-awareness)

---

### Issue #16 — Loom Demo Recording

**What:** Record a 2-3 minute video demonstrating the pipeline processing real data.

**Why:** An engineering manager won't clone your repo. They'll watch a 2-minute video. The video needs to show: real data flowing through → sanitizer catching real issues → health scores being generated → sync report with real numbers.

**Demo script:**
1. Show the raw data (real Amazon products with visible issues)
2. Run `docker-compose up` (one command)
3. Run the sync command
4. Watch the progress bar as 100K+ items are processed
5. Show the sync report (real numbers)
6. Show a DLQ item and explain why it failed
7. Show the `/metrics` endpoint live
8. End with: "Swap the base URL to point at a real Constructor endpoint. Zero code changes."

---

### Issue #17 — Docker Compose One-Command Setup

**What:** Ensure `docker-compose up` starts everything: pipeline server, mock Constructor API, optional Kafka + Zookeeper, optional Prometheus.

**Why:** "One-command setup" is a promise in the README. It needs to actually work — no pre-requisite installation steps beyond Docker itself.

**Services:**
- [ ] `constructsync` — the main pipeline API
- [ ] `constructor-mock` — the mock Constructor API server
- [ ] `kafka` + `zookeeper` — for event-driven mode (optional profile)
- [ ] Volume mounts for `data/`, `reports/`, `dlq/`

**Acceptance Criteria:** On a clean machine with only Docker installed, `docker-compose up` starts successfully within 60 seconds.

---

### Issue #18 — XSS Augmentation Script (Transparent, Documented)

**What:** A script that takes a real dataset and augments ~0.5% of records with realistic OWASP XSS payloads, transparently documented.

**Why:** Real Kaggle datasets don't contain XSS payloads (Amazon scrubs them). For the demo, we need to show the sanitizer actually catching things. But we do this transparently — the script is in the repo, the README explains exactly what it does, and the augmentation rate is documented.

**This is industry standard:** Security teams test WAFs by injecting known attack patterns into real traffic. We're doing the same thing for catalog data.

**Implementation:**
- [ ] Reads real dataset, injects OWASP payloads into `description` field of random 0.5% of rows
- [ ] Payloads sourced from the same OWASP vectors used in Issue #13
- [ ] Output CSV includes `augmented: true/false` column for traceability
- [ ] README documents: "Base dataset: Amazon Product Dataset 2020. Augmented with N records containing OWASP-standard XSS test vectors."

**Acceptance Criteria:** Augmented dataset passes through pipeline. Sync report shows correct sanitization count matching the number of augmented records.

---

## Summary

| Milestone | Issues | What You Have After |
|-----------|--------|-------------------|
| **1: Foundation** | #1-#3 | Project runs, mock API works, real data is downloadable |
| **2: Core Pipeline** | #4-#7 | Working ingestion pipeline with sanitization, concurrency, DLQ, and reports |
| **3: Data Intelligence** | #8-#10 | Best Buy live integration, health scoring, delta sync |
| **4: Advanced** | #11-#14 | Kafka, LLM backfill, OWASP test suite, Prometheus metrics |
| **5: Polish** | #15-#18 | Demo-ready with video, docs, and one-command setup |
