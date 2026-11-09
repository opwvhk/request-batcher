Request Batcher
===============

Library to collect requests into batches to reduce request overhead.

When dealing with a remote requests, request overhead can be significant. Especially for high
request volumes.

But for high volumes, it is often possible (desirable even) to trade a bit of extra latency for a
(much) higher throughput. This can be done by batching multiple requests together. This library
takes care of the lowlevel stuff around that. All you have to do is handle the batched requests.


Timing
------

Collecting multiple requests (to other servers) into batches means that multiple threads and
multiple servers are involved. This means we'll need to consider timeouts.

The following timeouts are available:

1. When enqueuing requests, you can specify the maximum time to block before failing.
2. A queue has a linger time: how long requests can wait on the queue to collect a larger batch
3. A queued request has a timeout: how long it may take to complete
4. When acquiring a batch, you specify the maximum timeout to wait for elements to arrive

The queue linger time and the timeout to acquire a batch influence eachother: when acquiring a
batch, the specified timeout is an upper bound. The remaining linger time of the elements on
the queue is honoured if possible.


Logging and Observations (Metrics and Traces)
---------------------------------------------

This library uses [SLF4J](https://www.slf4j.org/) for logging and
[micrometer](https://micrometer.io/) for _observations_ (this includes both metrics and traces).

Logs are written only using the logger name `net.fs.opk.batching.BatchRunner`, describing changes in
run state.

The following observations are exposed :

| Observation     | Observes                                      |
|-----------------|-----------------------------------------------|
| `request_batch` | The start of queuing until processing is done |
| `queue`         | The time spent on the queue                   |
| `processing`    | The time spent processing                     |

The observations carry a `BatchQueueContext`, with which an `ObservationConvention` can be used to
add tags. A default implementation, `net.fs.opk.batching.DefaultBatchQueueObservationConvention`,
adds the queue name as a tag to both metrics and traces.


Usage example
-------------

In this (contrived) example we'll batch numbered requests, returning a description. In real
scenario's you'd perform a network call.

```java
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import net.fs.opk.batching.BatchElement;
import net.fs.opk.batching.BatchQueue;
import net.fs.opk.batching.BatchRunnerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Example {
    static void main(String[] args) {

        // Step 0 (optional): the application should have a ObservationRegistry to handle metrics
        // and tracing.

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        SimpleTracer tracer = new SimpleTracer();

        ObservationRegistry observationRegistry = Observations.getGlobalRegistry();
        val observationConfig = observationRegistry.observationConfig();
		observationConfig.observationHandler(new TracingAwareMeterObservationHandler<>(
                new DefaultMeterObservationHandler(meterRegistry), tracer));
        observationConfig.observationHandler(new DefaultTracingObservationHandler(tracer));

        // Step 1 (configuration): create a BatchQueue, and start at least one BatchRunner to consume it

        BatchQueue<Integer, String> batchQueue = new BatchQueue<>(10_000,
                1, MILLISECONDS, 30, SECONDS);

        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.execute(BatchRunnerFactory.forConsumer(batchQueue, 5, batch -> {
            // Here, you'd probably implement a call to a bulk API...
            String suffix = " with " + (batch.size() - 1) + " other elements";
            for (BatchElement<Integer, String> element : batch) {
                element.success("Batched " + element.getInputValue() + suffix);
            }
        }));

        // Step 2 (using): use the queue to batch requests

        for (int request = 0; request < 1000; request++) {
            // Here, you'll want to implement your actual application
            CompletableFuture<String> futureResult = batchQueue.enqueue(request);
            futureResult.thenAccept(System.out::println);
        }

        // Step 3 (clean-up): to close your application, the queue must be shutdown
        // (after that, the BatchRunner will stop itself when the queue is empty)

        batchQueue.shutdown();
        // Optional: wait until the queue is empty and the BatchRunner has terminated.
        batchQueue.awaitShutdownComplete(100, MILLISECONDS);
        // Optional: shutdown the executor if you used a dedicated one.
        executor.shutdown();
    }
}
```

