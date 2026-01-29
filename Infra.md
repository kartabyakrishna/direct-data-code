Veeva Direct Data API → Amazon Redshift

Final Architecture & Workflow (Incremental, Log, Full Load)


---

1. Purpose & Scope

This document describes the final, production‑grade architecture for ingesting data from Veeva Direct Data API (DDAPI) into Amazon Redshift, supporting:

15‑minute incremental data loads

Daily log loads

Catastrophic full load recovery


The design guarantees:

Exactly‑once apply semantics

Strict ordering of incrementals

Transactional safety

Automatic pause and resume

Event‑driven execution (no polling delays)

No unnecessary full reloads


This document incorporates all scenarios, edge cases, and control‑plane rules discussed so far.


---

2. Core Design Principles (Non‑Negotiable)

1. One DDAPI window = one Redshift transaction


2. Incrementals must be applied strictly in stoptime order


3. Only Redshift COMMIT advances data truth


4. S3 is durable storage, not transactional state


5. DynamoDB is the single source of control‑plane truth


6. Producer must never block


7. Apply failures must block downstream apply


8. FULL load resets truth and rewinds incrementals


9. Control‑plane state is authoritative, not file arrival time




---

3. High‑Level Architecture

Separation of Concerns

Layer	Responsibility

Producer	Retrieve & extract DDAPI files
Storage	Durable landing of extracted files
Control Plane	Ordering, locking, precedence, recovery
Consumer	Transactional apply into Redshift



---

4. AWS Services Used

Concern	Service

Scheduling (producer)	EventBridge
DDAPI retrieval	ECS Fargate or Glue Batch
Durable storage	Amazon S3
Control plane	DynamoDB
Event‑driven wakeup	DynamoDB Streams + Lambda
Orchestration	Step Functions
Transform / Apply	AWS Glue
Data warehouse	Amazon Redshift
Alerting	SNS / PagerDuty / Slack



---

5. Data Types Supported

5.1 Incremental Data (15‑minute)

Generated every 15 minutes

Contains:

Manifest file (rows describe object, operation, file path)

Upsert CSVs

Delete CSVs



5.2 Log Data (Daily)

Generated once per day

Incremental in nature

Independent of fact tables

Lower precedence than incrementals


5.3 Full Load (Catastrophic Recovery)

Generated once per day

Snapshot valid only as of 00:00 UTC

Replaces all existing warehouse data



---

6. S3 Layout (Raw Zone)

s3://ddapi-raw/
  vault=X/
    incr/stoptime=YYYYMMDDHHMM/
      manifest.csv
      objectA_upsert.csv
      objectA_delete.csv
    log/date=YYYYMMDD/
      log_manifest.csv
      log_data.csv
    full/date=YYYYMMDD/
      full_manifest.csv
      full_data.csv


---

7. DynamoDB Control Plane

7.1 Queue Table: ddapi_queue

Attribute	Description

vault_id (PK)	Vault identifier
sort_key (SK)	stoptime / logical time
load_type	INCR / LOG / FULL
status	READY / PROCESSING / FAILED / APPLIED
s3_prefix	Input data location
checksum	Manifest checksum
attempt_count	Retry tracking
last_error	Failure reason


Ordering is enforced by sort_key.


---

7.2 Vault State Table: ddapi_vault_state

Attribute	Description

vault_id (PK)	Vault
mode	INCREMENTAL / FULL_LOAD
last_applied_stoptime	Logical watermark
current_epoch	Generation counter
full_load_started_at	Audit



---

8. Producer Workflow (Incremental & Log)

Trigger

EventBridge (every 15 minutes for INCR, daily for LOG)


Steps

1. Call DDAPI endpoint


2. Download tar.gz


3. Extract all files


4. Validate completeness


5. Write extracted CSVs to S3


6. Compute checksum


7. Insert queue entry:



status = READY
load_type = INCR | LOG

Failure Handling

Retrieval or extract failure:

No queue entry created

No downstream impact




---

9. Consumer Wakeup Mechanism

DynamoDB Streams enabled on ddapi_queue

Stream triggers Lambda ("waker")

Lambda checks:

No window in PROCESSING


If eligible → start Step Function execution


This ensures immediate resume after unblock, no polling.


---

10. Consumer Selection Logic (Critical)

General Rule

> Consumer always inspects the earliest non-APPLIED window.



Decision Table

Condition	Action

status = READY	Process
status = FAILED	STOP
status = PROCESSING	STOP


READY windows after a FAILED window are ignored.


---

10A. Step Functions – Consumer Orchestration (Detailed)

Purpose

AWS Step Functions acts as the durable, serialized consumer controller. It guarantees:

Exactly one active consumer per vault

Ordered execution of DDAPI windows

Deterministic retries

Clean failure boundaries


Step Functions does not perform data processing itself; it coordinates Glue jobs and state transitions.


---

State Machine Overview

StartExecution
  ↓
AcquireLock (DynamoDB conditional update)
  ↓
ReadVaultState
  ↓
BranchByMode (FULL_LOAD | INCREMENTAL)
  ↓
SelectNextWindow
  ↓
[NoEligibleWindow] → ExitSuccess
  ↓
MarkProcessing
  ↓
RunGlueApplyJob
  ↓
OnSuccess → MarkApplied → Loop
  ↓
OnFailure → MarkFailed → Alert → ExitFailure


---

State Details

1. AcquireLock

DynamoDB conditional update

Ensures no parallel Step Function execution for same vault


Retry:

MaxAttempts: 3

BackoffRate: 2.0


Failure:

Exit gracefully (another consumer already running)



---

2. ReadVaultState

Read ddapi_vault_state

Fetch:

mode (FULL_LOAD / INCREMENTAL)

last_applied_stoptime

current_epoch



No retries needed (strongly consistent read).


---

3. BranchByMode

If mode = FULL_LOAD:

Skip INCR and LOG

Route to FULL processing


If mode = INCREMENTAL:

Route to INCR processing



---

4. SelectNextWindow

Query DynamoDB:

Partition: vault_id

Sort ascending by stoptime

Filter:

status != APPLIED

epoch == current_epoch



Decision:

READY → proceed

FAILED → STOP

PROCESSING → STOP



---

5. MarkProcessing

DynamoDB conditional update:

SET status = PROCESSING
IF status = READY

Guarantees single ownership.

Retry:

MaxAttempts: 2



---

6. RunGlueApplyJob

Invoke Glue job synchronously.

Parameters passed:

vault_id

load_type (INCR / LOG / FULL)

s3_prefix

stoptime

epoch


Glue job timeout configured explicitly.

Retry:

MaxAttempts: 1 (no blind retries for data operations)



---

7. MarkApplied

After Glue success:

Update queue entry:

status = APPLIED


Update vault_state.last_applied_stoptime


Then loop back to SelectNextWindow.


---

8. OnFailure Path

If Glue fails:

Update queue entry:

status = FAILED

last_error populated


Publish alert (SNS)

Exit Step Function


No further processing occurs.


---

10B. Retry & Backoff Strategy (End-to-End)

Retrieval / Producer

Exponential backoff on DDAPI HTTP failures

No queue entry created until full extraction success


Step Functions

Lock acquisition retries

No infinite retries

Fail fast on data-plane errors


Glue Apply Job

No internal retries for COPY / MERGE

Failure is surfaced immediately


Support-triggered retries

Manual reset FAILED → READY

DynamoDB Stream wakes consumer instantly



---

10C. Infrastructure Hardening (Failure Prevention)

Glue Job Configuration

Worker type: G.2X or higher (avoid disk pressure)

NumberOfWorkers sized for worst-case FULL load

Enable Job Bookmarking = OFF (CDC is external)


Ephemeral Storage

Use --additional-python-modules sparingly

Avoid large local temp files

Stream CSVs directly from S3



---

ECR & Image Pull Stability

For Glue (or ECS-based jobs):

Use private ECR with VPC endpoints

Enable:

com.amazonaws.region.ecr.api

com.amazonaws.region.ecr.dkr


Pre-warm images where possible

Avoid latest tag; pin digests



---

IAM & Auth Resilience

Use IAM roles, never static creds

Least-privilege policies

Separate roles for:

Producer

Consumer

Glue




---

Network Resilience

Glue jobs run inside VPC

Required endpoints:

S3 (Gateway)

DynamoDB (Gateway)

Logs (Interface)

ECR (Interface)



No public internet dependency.


---

Redshift Stability

RA3 or Serverless

Concurrency scaling enabled

COPY from S3 using IAM role

Explicit transaction boundaries



---

10D. Alerting & Observability

Alerts Triggered On

Window status → FAILED

Glue job failure

Step Function failure

FULL load failure


Metrics

Lag (now - last_applied_stoptime)

Queue depth

Apply duration per window



---

---------|--------| | status = READY | Process | | status = FAILED | STOP | | status = PROCESSING | STOP |

READY windows after a FAILED window are ignored.


---

11. Redshift Apply (Transactional Core)

Structure

staging schema (inside Redshift)

final schema


Per‑Window Apply Logic

BEGIN;

TRUNCATE staging tables;
COPY staging tables FROM S3;
APPLY deletes;
MERGE upserts;

COMMIT;

On any error:

ROLLBACK;

Only after COMMIT:

status = APPLIED
last_applied_stoptime updated


---

12. Failure Handling & Stopping Semantics

Retrieval Failure

No queue entry

Nothing stops


Apply Failure

status → FAILED

Consumer refuses to advance

Producer continues

READY backlog accumulates safely

Alert raised


Stopping is logical, not physical.


---

13. Daily Log Load Handling

load_type = LOG

LOG loads:

Do not block INCR

Can be processed independently

Lower precedence



Optionally run LOG on a separate consumer.


---

14. Full Load Workflow (Catastrophic Recovery)

14.1 Triggering FULL Load

Manual trigger (API Gateway / CLI)

Not EventBridge


On trigger:

1. Determine snapshot boundary:

full_snapshot_time = YYYY‑MM‑DD 00:00 UTC


2. Rewind control plane:

For all INCR where stoptime > boundary and status = APPLIED → set status = READY



3. Increment current_epoch


4. Set:

vault.mode = FULL_LOAD
vault.last_applied_stoptime = boundary




---

14.2 FULL Load Apply

Consumer ignores INCR and LOG

Processes FULL only


Redshift logic:

BEGIN;
TRUNCATE / DROP final tables;
COPY full dataset;
COMMIT;


---

14.3 Post‑FULL Resume

After FULL success:

vault.mode = INCREMENTAL
vault.last_applied_stoptime = 00:00

Queue state:

INCR 00:15 → READY
INCR 00:30 → READY
...

Consumer resumes from 00:15 onward.


---

15. Why Rewind Is Mandatory

If FULL occurs after incrementals were already APPLIED:

Redshift truth is reset to 00:00

Control plane must be corrected

Incrementals beyond boundary must re‑run


Without rewind → guaranteed data loss.


---

16. Precedence Rules

Load Type	Priority

FULL	Highest
INCR	Medium
LOG	Lowest


FULL blocks everything until success.


---

17. Epoch Safety (Recommended)

Each queue item has epoch

Vault has current_epoch

Consumer only processes matching epoch


Prevents stale PROCESSING states after FULL.


---

18. Support Runbook (Summary)

1. Alert received (FAILED window)


2. Investigate logs


3. Fix root cause


4. Reset status FAILED → READY


5. System resumes automatically



For FULL:

1. Trigger FULL


2. Monitor


3. Verify resume




---

19. Final Invariants (Must Always Hold)

1. Producer never blocks


2. One window applied at a time


3. Watermark advances only on COMMIT


4. FAILED window blocks progress


5. FULL resets truth and rewinds incrementals


6. Event‑driven resume, no polling




---

20. Final Mental Model

> DDAPI files are messages.
S3 is memory.
DynamoDB is the brain.
Redshift is the source of truth.




---

This architecture is complete, consistent, and production‑safe.

Done. I’ve extended architecture.md with a deep, production-grade Step Functions section and hardened infrastructure guidance.

What I added (important highlights)

Full Step Functions state machine (state-by-state)

Locking, mode branching, selection, looping, failure exits


Exact retry & backoff strategy

Where retries are allowed

Where they are explicitly not allowed (data safety)


Glue job hardening

Worker sizing

Ephemeral storage avoidance

Bookmarking disabled (correct for CDC)


ECR auth / image-pull failure prevention

VPC endpoints for ecr.api + ecr.dkr

Digest-pinned images


Network & IAM resilience

Required VPC endpoints (S3, DynamoDB, Logs, ECR)

Role separation


Redshift stability practices

Transaction discipline

Concurrency scaling


Alerting & observability

What alerts fire, when, and why

Lag & backlog metrics



This now reads like a real internal platform design doc that can survive:

Infra review

Security review

On-call handoff

Catastrophic recovery scenarios


If you want next, I can:

Turn this into Terraform modules (Step Functions, DynamoDB, Glue)

Add exact IAM policies per role

Provide Glue job pseudocode aligned to accelerator logic

Produce a failure-mode table (cause → symptom → mitigation)


Just tell me what you want to lock down next.
