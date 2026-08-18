# Solution

## What was broken and why

The webhook ingestion path had several correctness and lifecycle problems.

1. **Duplicate deliveries were not safely idempotent.** The code first checked `EventExists(event_id)` and then inserted the event as two separate operations. Concurrent redeliveries could both observe that the event did not exist and both insert it. The database also had only a normal index on `event_id`, not a uniqueness constraint. This could cause duplicate event rows and double-count account statistics.

   The fix is database-backed idempotency: `events.event_id` is now `UNIQUE`, and the insert uses `ON CONFLICT (event_id) DO NOTHING`. The insert result tells the service whether it actually accepted a new event. Only newly inserted events update calls and account statistics. This makes the check-and-insert decision atomic.

2. **Account statistics could drift because duplicate events could reach the counting logic.** `IncrementAccountStats` and the in-memory cache both incremented totals for every event that passed the old duplicate check. With the idempotency fix, duplicate deliveries return before these operations, so one `event_id` contributes to the statistics only once.

3. **Recording processing used the HTTP request context for asynchronous work.** Recording processing runs in a goroutine after the webhook response can finish. The goroutine originally reused `r.Context()`. Once the HTTP request completed, that context could be cancelled, causing `MarkRecordingProcessed` to fail with `context canceled`. The error was also previously discarded, so the failure was invisible in the logs. The background job now uses an independent context and logs processing errors.

4. **In-flight recording work was not tracked during shutdown.** Recording processing was launched as an unmanaged goroutine, so application shutdown had no way to wait for it. A `sync.WaitGroup` now tracks recording jobs. `Service.Wait()` waits for all in-flight recording work, and the server calls it during shutdown before the process exits.

Regression tests were added for concurrent duplicate delivery, statistics not being double-counted, successful asynchronous recording processing, and waiting for in-flight recording work.

## Deduplication choice

I chose **PostgreSQL uniqueness plus `ON CONFLICT DO NOTHING`** rather than Redis-based deduplication. `event_id` is durable state and PostgreSQL is already the source of truth for the stored events and account statistics. A unique constraint gives a database-enforced guarantee even when deliveries arrive concurrently, while `ON CONFLICT DO NOTHING` makes the operation atomic and avoids turning a normal redelivery into a 500 response.

A Redis-only check would add another state store and still require careful atomicity/TTL handling. A separate application-level lock would not provide the same durable guarantee and would be more complicated across multiple service instances.

## At 10,000 webhooks/sec

At 10,000 webhooks/sec, I would keep the database uniqueness constraint as the final correctness boundary but move slow/non-critical processing behind a durable queue or streaming system. The HTTP path should do minimal synchronous work: validate the event, perform the idempotent durable write, and acknowledge quickly. Recording processing should be handled by scalable workers with bounded concurrency, retries, backoff, and durable job state.

I would also review PostgreSQL connection-pool sizing, partition/index strategy, batching where appropriate, observability, and Redis usage for read-heavy statistics. Redis could be useful as a cache, but it should not replace the database uniqueness guarantee for idempotency.
