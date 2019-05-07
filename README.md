# Request Batcher

For many systems, batching requests together amplifies throughput. This library takes care of the basics when batching multiple requests together. All you have to do is handle the batched requests.


## Usage example

In this (contrived) example, we'll batch numbered requests, returning a description. In real scenario's, you'd perform a network call.

````java
import net.fs.opk.batching.BatchQueue;
import net.fs.opk.batching.BatchElement;
import net.fs.opk.batching.BatchRunner;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Example {
    public static void main(final String[] args) throws InterruptedException {

        // Step 1 (configuration): create a BatchQueue, and start at least one BatchRunner to consume it

        BatchQueue<Integer, String> batchQueue = new BatchQueue<>(10_000, 1, MILLISECONDS);

        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.execute(new MyBatchRunner(batchQueue));


        // Step 2 (using): use the queue to batch requests

        for (int request = 0; request < 1000; request++) {
            // Here, you'll want to implement your actual application
            CompletableFuture<String> futureResult = batchQueue.enqueue(request);
            futureResult.thenAccept(System.out::println);
        }


        // Step 3 (cleanup): to close your application, the queue must be shutdown
        // (after that, the BatchRunner will stop itself when the queue is empty)

        batchQueue.shutdown();
        executor.shutdown();
        // Optional: wait until the queue is empty and the BatchRunner has terminated.
        batchQueue.awaitShutdownComplete(100, MILLISECONDS);
        executor.awaitTermination(1, MILLISECONDS);
    }
}

class MyBatchRunner extends BatchRunner<Integer, String> {
    MyBatchRunner(BatchQueue<Integer, String> queue) {
        super(queue, 5, 100);
    }

    protected void executeBatch(final List<BatchElement<Integer, String>> batch) {
        // Here, you'd probably implement a call to a bulk API...
        String suffix = " with " + (batch.size() - 1) + " other elements";
        for (BatchElement<Integer, String> element : batch) {
            element.success("Batched " + element.getInputValue() + suffix);
        }
    }
}
````

