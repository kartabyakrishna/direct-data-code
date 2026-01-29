

Final Design: Veeva Direct Data API → Amazon Redshift

(Robust, Sequential, Transactional, Event-Driven)


---

1. Design Objectives (Non-negotiable)

1. Exactly-once apply semantics per DDAPI incremental window (stoptime)


2. Strict ordering of incremental windows


3. One 15-minute window = one Redshift transaction


4. No full reloads on failure


5. Automatic pause & resume on failures


6. Event-driven recovery (no waiting for schedulers)


7. Only one Glue job running at a time


8. Producer failures must not block ingestion


9. Apply failures must block downstream apply


10. Operationally simple for support teams




---

2. High-Level Architecture

┌─────────────────────────┐
                   │        EventBridge       │
                   │   (every 15 min + lag)   │
                   └───────────┬─────────────┘
                               ↓
                 ┌─────────────────────────────┐
                 │  Producer Job (ECS / Glue)  │
                 │  - Call DDAPI               │
                 │  - Download tar.gz          │
                 │  - Extract CSVs             │
                 │  - Validate & checksum      │
                 └───────────┬─────────────────┘
                             ↓
          ┌────────────────────────────────────────┐
          │                 Amazon S3               │
          │  raw-ddapi/vault=X/stoptime=YYYYMMDDHHMM│
          │   - manifest.csv                        │
          │   - object_upsert.csv                   │
          │   - object_delete.csv                   │
          └───────────┬────────────────────────────┘
                      ↓
          ┌────────────────────────────────────────┐
          │              DynamoDB Queue             │
          │  (ordered by vault_id + stoptime)      │
          │  status: READY | PROCESSING | FAILED    │
          │          | APPLIED                      │
          └───────────┬────────────────────────────┘
                      ↓   (Streams)
          ┌────────────────────────────────────────┐
          │           DynamoDB Stream               │
          └───────────┬────────────────────────────┘
                      ↓
          ┌────────────────────────────────────────┐
          │          Lambda (Waker)                 │
          │  - If no PROCESSING item exists        │
          │  - Trigger Step Function               │
          └───────────┬────────────────────────────┘
                      ↓
          ┌────────────────────────────────────────┐
          │        Step Functions (Consumer)        │
          │  - Claim next window                   │
          │  - Run Glue Apply Job                  │
          │  - Loop until blocked or empty         │
          └───────────┬────────────────────────────┘
                      ↓
          ┌────────────────────────────────────────┐
          │             Glue Apply Job              │
          │  - COPY into Redshift staging schema    │
          │  - MERGE into final tables              │
          │  - One transaction per stoptime         │
          └────────────────────────────────────────┘


---

3. Producer Path (Time-Driven, Non-Blocking)

Trigger

EventBridge every 15 minutes (with a small delay, e.g. +3–5 min)


Responsibilities

1. Call Veeva DDAPI incremental endpoint


2. Download .tar.gz


3. Extract all files


4. Write extracted files to S3:

s3://raw-ddapi/vault=X/stoptime=T/
  - manifest.csv
  - <object>_upsert.csv
  - <object>_delete.csv


5. Compute checksum of manifest


6. Insert DynamoDB record:



PK: vault_id
SK: stoptime
status = READY
s3_prefix
checksum
attempt_count = 0

Failure handling

Retrieval / extract failure → no DynamoDB entry

No downstream impact

Next scheduled run retries


> Producer never blocks because of Redshift or apply failures.




---

4. DynamoDB Queue (Single Source of Truth)

Table: ddapi_queue

Attribute	Purpose

vault_id (PK)	Partition
stoptime (SK)	Ordering
status	READY / PROCESSING / FAILED / APPLIED
s3_prefix	Input data location
checksum	Integrity
attempt_count	Retry tracking
last_error	Diagnostics


Ordering Rule

> Windows must be applied strictly in ascending stoptime order.




---

5. Consumer Path (Event-Driven, Serialized)

Trigger

DynamoDB Streams → Lambda → Step Functions


Lambda “Waker” Logic

1. Triggered on:

New item with status = READY

Update from FAILED → READY

Update from PROCESSING → APPLIED



2. Check:

Is any window currently PROCESSING?



3. If no, start Step Function execution (one per vault)




---

6. Consumer Selection Rule (Critical Invariant)

When the consumer runs:

1. Query earliest window where status != APPLIED


2. Decision:

READY → process

FAILED → STOP (exit)

PROCESSING → STOP (another run active)




> READY windows after a FAILED window are ignored until the FAILED one is resolved.



This is how ordering and safety are enforced.


---

7. Glue Apply Job (Transactional Core)

Redshift Structure

staging schema (inside Redshift)

final schema


Apply Logic (Per stoptime)

BEGIN;

TRUNCATE staging tables;

COPY staging tables FROM S3;

APPLY deletes;

MERGE upserts into final tables;

COMMIT;

Guarantees

One stoptime = one transaction

On any error → ROLLBACK

No partial state visible

Safe retries



---

8. Failure Handling & “Stopping” Mechanism

Retrieval Failure

No queue entry created

Nothing stops

Retry later


Apply Failure

Window marked FAILED

Consumer stops advancing

Producer continues ingesting

READY backlog accumulates safely

Alert sent to support


Nothing is physically turned off.
Progress simply refuses to advance.


---

9. Support Team Recovery Flow

1. Alert received (FAILED window)


2. Root cause fixed (DDL, IAM, data issue, etc.)


3. Support runs:

Update status FAILED → READY for that stoptime


4. DynamoDB Stream fires


5. Lambda triggers Step Function


6. Consumer resumes automatically


7. Backlog processed sequentially


8. System catches up to real time



No scheduler restart. No full reload.


---

10. Enforcement of “Only One Glue Job”

Guaranteed by:

1. DynamoDB conditional update:

READY → PROCESSING (only if READY)


2. Consumer rule: only earliest non-APPLIED window is eligible


3. Optional Step Function execution name per vault



This is sufficient and safe.


---

11. Final Invariants (Must Always Hold)

1. Producer never blocks


2. Consumer applies exactly one window at a time


3. Watermark advances only after Redshift COMMIT


4. No window after a FAILED window may be applied


5. READY ≠ allowed to process


6. Stopping = refusing to advance state, not disabling infra




---

12. Final Mental Model

> DDAPI windows are messages.
S3 is durable memory.
DynamoDB is the control plane.
Redshift is the transaction boundary.
Progress is explicit and intentional.



