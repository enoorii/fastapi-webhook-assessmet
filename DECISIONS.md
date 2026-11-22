# Architecture & Debugging Decisions

Please fill out the four sections below concisely before submitting your solution.

| Requirement                              |                                                                                                                   Target |
| ---------------------------------------- | -----------------------------------------------------------------------------------------------------------------------: |
| Ingestion latency                        |                                                                                                         p95 below 250 ms |
| Health endpoint latency during the burst |                                                                                                  Every probe below 50 ms |
| Idempotency                              | Concurrent retries with the same idempotency key must not create duplicate database rows or duplicate downstream effects |
| Delivery reliability                     |                                   Transient downstream `503` responses must be retried until unique events are delivered |
| Infrastructure                           |                                                        Python and SQLite only; no Celery, Redis, RabbitMQ, or PostgreSQL |

## 1. Root-Cause Diagnosis

What specific bottlenecks and failure modes did you observe in the original codebase under concurrent load?

- **Event-loop / CPU behavior:**
  - The `scrub_and_fingerprint` is a CPU intensive function, and it is blocking event loop
    - make it async and await on it is not ideal. there is no cpu _wait_ time here. (no IO)
    - So, best thing is to offload it to another thread

- **Concurrency & race conditions:**
  - Not using the idempotency key
    - Though the initial code checked that idempotency key exist, it doesn't use it
    - Multiple identical requests could start multiple independent process
    - This created race condition problem
    - I solved this by using separating accept/delivery routines
      - Input data is accepted and saved into database _if_ a look up doesn't reveal it already exists
    - Either way, a response is returned from ingest endpoint that shows request is accepted

- **Resource & connection management:**
  - Downstream requests are send _sequentially_
  - So if an entry causes a problem, the program will not move on with next entry
  - To do this I used multiple httpx clients, each run in a asyncio task
  - To prevent a single entry handled by multiple clients I used a 3 level verification, condition to pick up a task is:
    - Its state is _pending_
    - Its state is _processing_ and at least 10 seconds is passed since them the time it was picked up
    - If maximum retry is reached no further
      - When a process attempt to get a due task, database is locked to prevent race condition (no other process takes that)

## 2. Concurrency & Offloading Strategy

How did you separate CPU-bound preprocessing, HTTP acknowledgment, and downstream delivery? Why did you choose your specific concurrency primitives over alternatives (e.g., `asyncio.to_thread` vs. `ProcessPoolExecutor` vs. FastAPI `BackgroundTasks`)?

_my answer_
FastAPI background tasks lives in same thread as API. So, doing a CPU-intensive task can still block api thread and has a negative effect on its responsiveness.

`asyncio.to_thread` offload task to another thread. But python doesn't allow multiple threads run the code in parallel. So, this solve the problem of blocking fastapi event loop. But it is still run on a single CPU-core.

To improve performance we use _ProcessPoolExecutor_ which runs complete independent processes. This can improve throughput under intense load.

> [!note] A Note On Workers And Process-Overhead
>
> 1. Running uvicorn with `--workers 4` give us four event loops, but each of them can still be blocked by CPU-intensive task. So, we still need process pool.
> 2. Task takes 50-60ms, according to my search this is much higher that the 10ms floor that justifies process pool overhead

## 3. Idempotency & SQLite Concurrency

How does your implementation guarantee zero duplicate processing when two identical webhook retries arrive at the exact same millisecond? What SQLite settings or constraints did you rely on?

> [!info] Saving Idempotency Key With Unique Constraint
> The ingest function takes idempotency key. That key must be used to prevent duplicate runs.
> To do that I added the key to sqlite database. Using a `UniqueConstraint` check that make sure there are no duplicates.

> [!info] read-before-write
> I found this pattern while analyzing cumulative insert times in database. They got worse for each request under heavy lead.
> So, I used this pattern and did a fast read to check if idempotency key exists. This improved performance by 30ms.
> Database constraint is still valid to handle _exact same milisecond_ scenario.

## 4. Failure Modes & Production Trade-offs

- What happens in your design if the downstream service is completely unreachable for 60 seconds?

- What happens if this service process crashes 50ms after acknowledging a webhook?

- What would break first if we deployed this container with `--workers 4` instead of 1?

> [!question] What happens in your design if the downstream service is completely unreachable for 60 seconds?
> Worker processes retry the request when they receive network/connection error.
> But if they receive `4xx` responses for things like validation error, authorization error, ... They don't retry. And mark task as _failed_

- Delivery task remains in database with _pending_ condition, later on a scheduled script retry them.
- When a worker process start a pending operation, it changes the condition to _processing_ so it is not picked up twice.

> [!question] What happens if this service process crashes 50ms after acknowledging a webhook?
> Acknowledgment is only done when data is saved in database (alongside its idempotency key)
>
> So even if program crashes, database has all the data we need to continue with that entry (downstream process)

> [!question] What would break first if we deployed this container with `--workers 4` instead of 1?
> I think the first problem is with sqlite database. It has a write lock. Multiple worker processes compete for connection lock and will increase its latency.
>
> Another thing that comes into my mind is that the delivery workers we setup (which send requests using httpx and process pool) will multiply by 4. Causing the overhead for many processes adds up.
>
> So, for this specific problem, maybe a single uvicorn worker with process pool for cpu-intensive task and downstream handling is a better choice.
>
> Also if we are using PostgreSQL that doesn't block on write and also a dedicated task runner like celery, the choice can be quite different.
