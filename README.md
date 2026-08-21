# Bulk Data Import Pipeline

A .NET 8 Web API demonstrating how to take a naive, slow CSV import process and evolve it — step by step — into a production-grade, high-throughput, fault-tolerant bulk import pipeline.

Built as a hands-on learning project to explore streaming I/O, the producer/consumer pattern with `System.Threading.Channels`, `SqlBulkCopy`, staging-table + `MERGE` patterns, validation/error reporting, and idempotency via file hashing and checkpointing.

## Tech Stack

- **.NET 8** / ASP.NET Core Web API
- **Entity Framework Core** (SQL Server provider) — used for the baseline and domain model
- **Microsoft.Data.SqlClient** — `SqlBulkCopy` for high-throughput inserts
- **System.Threading.Channels** — bounded channel for producer/consumer streaming
- **SQL Server** — staging table + `MERGE` for safe, idempotent loading

## The Problem

Importing large CSV files (hundreds of thousands of rows) into a relational database is deceptively easy to get wrong. A naive implementation — read a row, insert a row, repeat — works fine in a demo with 100 rows and falls over completely at real-world scale. This project incrementally builds up the techniques needed to go from "works on my machine with a small file" to "handles 500K+ rows reliably, quickly, and safely."

## Evolution of the Pipeline

### 1. Baseline — Row-by-Row EF Core Insert (intentionally slow)
Reads the CSV line by line and calls `SaveChangesAsync()` after every single row. Demonstrates why this approach is a performance anti-pattern: every row is a separate round trip to the database, and EF Core's change tracker grows and slows down as more entities are tracked in a single `DbContext` lifetime.

### 2. Producer/Consumer Streaming with Channels
Introduces `System.Threading.Channels` to decouple **reading** the file from **writing** to the database. A producer task parses the CSV and pushes rows into a bounded channel; a consumer task reads from the channel and writes to the database in batches, clearing the EF Core change tracker after each batch. Producer and consumer run concurrently, and the channel's bounded capacity applies backpressure so the producer can't outrun the consumer and blow up memory.

### 3. SqlBulkCopy + Staging Table + MERGE
Replaces EF Core inserts with `SqlBulkCopy`, which uses SQL Server's native bulk-load protocol instead of individual `INSERT` statements. Rows are first bulk-loaded into a `CustomersStaging` table (isolating raw incoming data from the production table), then a `MERGE` statement reconciles staging data into the final `Customers` table — inserting new rows and updating existing ones matched by email. This also makes re-running the same import **idempotent**: importing the same file twice does not create duplicates.

### 4. Validation + Error Reporting
Real-world CSVs are messy. Every row is validated before it reaches the database (missing fields, malformed dates, malformed booleans, wrong column counts). Invalid rows are never sent to the channel — they're collected with a specific reason and line number, and written out as a downloadable error-report CSV at the end of the run. Valid rows still import successfully even when a percentage of the file is bad data.

### 5. Deduplication (File Hash) + Checkpointing
Every import computes a SHA-256 hash of the source file and records it, along with row-level progress, in an `ImportJobs` tracking table. If the exact same file is submitted again after a successful import, the pipeline detects the duplicate via hash lookup and skips reprocessing entirely — turning a multi-second (or longer) operation into a sub-200ms lookup. Progress checkpoints are written after every batch, so incomplete/crashed jobs are identifiable in the tracking table (`Status = 'InProgress'`) rather than silently disappearing.

## Performance Results

| Approach | Rows | Time | Relative Speed |
|---|---:|---:|---|
| Row-by-row EF Core (`/api/import/slow`) | 50,000 | 734.3 sec | 1x (baseline) |
| Channel-based producer/consumer + batching (`/api/import/channel`) | 50,000 | 8.0 sec | ~91x faster |
| SqlBulkCopy + Staging + MERGE (`/api/import/bulkcopy`) | 50,000 | 1.28 sec | ~574x faster |
| **Validated + resumable pipeline (`/api/import/resumable`)** | **500,000** | **14.25 sec** | Production-grade, full validation + idempotency, at 10x the row count |

At 500,000 rows, the resumable pipeline processed 490,014 valid rows and safely quarantined 9,986 intentionally-malformed rows into a downloadable error report — with zero crashes and zero duplicate records on re-run.

**Re-import of an already-processed file (deduplication check):** ~130ms, vs. several seconds for a full reprocess — confirmed via SHA-256 file-hash lookup against the `ImportJobs` table.

### Why the speedup happens
- **Batching** — collapsing thousands of individual `INSERT`/`SaveChanges` calls into a handful of bulk operations removes the dominant cost: network round trips.
- **`SqlBulkCopy`** — bypasses EF Core's per-row overhead (change tracking, validation, SQL generation) and uses SQL Server's native bulk-load (TDS bulk protocol).
- **Producer/Consumer overlap** — file I/O (reading/parsing) and database I/O (writing) happen concurrently instead of sequentially.
- **Change tracker discipline** — clearing EF Core's change tracker after each batch prevents the degradation seen in the baseline, where per-row latency actually got *worse* as the run progressed (4.5ms/row at 1,000 rows → 14.7ms/row at 50,000 rows).

## Architecture

```
CSV File
   │
   ▼
[Producer Task] ── parses & validates each row ──► invalid rows ──► in-memory error list
   │
   │ valid rows only
   ▼
[Bounded Channel] (backpressure-controlled queue)
   │
   ▼
[Consumer Task] ── batches rows (5,000/batch) ──► SqlBulkCopy ──► CustomersStaging table
   │
   ▼
MERGE staging → Customers (insert new / update existing, matched on Email)
   │
   ▼
ImportJobs table updated (checkpoint, row counts, status = Completed)
   │
   ▼
Error report CSV written (if any invalid rows) — downloadable via /api/import/download-errors
```

## Key Tables

- **`Customers`** — final destination table.
- **`CustomersStaging`** — temporary landing table for each import batch; truncated at the start of every run.
- **`ImportJobs`** — tracks every import attempt: source file hash (for deduplication), row counts, checkpoint progress, and status (`InProgress` / `Completed`).

## API Endpoints

| Endpoint | Purpose |
|---|---|
| `POST /api/DataGenerator/generate?rowCount=N` | Generates a synthetic CSV of N customer rows (includes a small % of intentionally invalid rows) |
| `POST /api/Import/slow?fileName=X` | Baseline row-by-row EF Core import |
| `POST /api/Import/channel?fileName=X` | Producer/consumer channel-based import (EF Core writes) |
| `POST /api/Import/bulkcopy?fileName=X` | SqlBulkCopy + staging + MERGE, no validation |
| `POST /api/Import/validated?fileName=X` | Adds row validation + error CSV reporting on top of bulk copy |
| `POST /api/Import/resumable?fileName=X` | Full pipeline: validation + bulk copy + MERGE + file-hash dedup + checkpointing |
| `GET /api/Import/download-errors?filePath=X` | Downloads a generated error report CSV |

## What I'd Add Next (Production Roadmap)

- **True resume-from-checkpoint**: currently the pipeline tracks progress and can identify incomplete jobs, but a crashed job still needs to be re-run from the start. A full implementation would seek to the last checkpointed line and skip already-processed rows.
- **Parallel bulk-copy workers**: run multiple `SqlBulkCopy` writers against different batches concurrently (with care around staging table contention).
- **Configurable batch sizes and channel capacity** exposed via `appsettings.json` rather than hard-coded constants.
- **Structured logging** (e.g. Serilog) instead of relying purely on the API response payload for observability.

## Running Locally

1. Update the connection string in `appsettings.json` to point at your SQL Server instance.
2. Run EF Core migrations to create the `Customers` table (`Update-Database` in Package Manager Console).
3. Run the SQL script in `/Sql/CreateStagingAndJobTables.sql` (or the statements documented above) to create `CustomersStaging` and `ImportJobs`.
4. Run the project, generate a test CSV via `/api/DataGenerator/generate`, and try each import endpoint via Swagger.
