# Request Batcher

For many systems, batching requests together amplifies throughput. This library takes care of the basics when batching multiple requests together. All you have to do is handle the batched requests.


## Usage example

In this (contrived) example, we'll batch number requests, and add the number of elements in the batch.

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
        // Step 1: create a BatchQueue
        BatchQueue<Integer, String> batchQueue = new BatchQueue<>(10_000, 1, MILLISECONDS);

        ExecutorService executor = Executors.newFixedThreadPool(1);
        // Step 2: create (and start) at least one BatchRunner to consume the queue
        executor.execute(new MyBatchRunner(batchQueue));

        // Step 3: use the queue to batch requests
        for (int i = 0; i < 1000; i++) {
            batchQueue.enqueue(i).thenAccept(System.out::println);
        }

        // Step 4: when done, shutdown the queue (the BatchRunner will continue to consume the queue until it's empty, then shutdown)
        batchQueue.shutdown();

        // Cleanup
        batchQueue.awaitShutdownComplete(100, MILLISECONDS);
        executor.shutdown();
    }
}

class MyBatchRunner extends BatchRunner<Integer, String> {
    MyBatchRunner(BatchQueue<Integer, String> queue) {
        super(queue, 5, 100);
    }

    protected void executeBatch(final List<BatchElement<Integer, String>> batch) {
        String suffix = " with " + (batch.size() - 1) + " other elements";
        for (BatchElement<Integer, String> element : batch) {
            element.success("Batched " + element.getInputValue() + suffix);
        }
    }
}
````

