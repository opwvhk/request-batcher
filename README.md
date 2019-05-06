# Request Batcher

For many systems, batching requests together amplifies throughput. This library takes care of the basics when batching multiple requests together. All you have to do is handle the batched requests.


## Usage example

In this (contrived) example, we'll batch number requests, and add the number of elements in the batch.

````java
import net.fs.opk.batching.BatchQueue;
import net.fs.opk.batching.BatchElement;
import net.fs.opk.batching.BatchRunner;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Example {
    public static void main(final String[] args) throws InterruptedException {
        BatchQueue<Integer, String> batchQueue = new BatchQueue<>(10_000, 1, MILLISECONDS);

        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.execute(new MyBatchRunner());

        for (int i=0; i < 1000; i++) {
            batchQueue.enqueue(i).thenAccept(System.out::println);
        }

        batchQueue.shutdown();
        batchQueue.awaitShutdownComplete(100, MILLISECONDS);
        executor.shutdown();
    }
}

class MyBatchRunner extends BatchRunner<Integer, String> {
    MyBatchRunner(BatchQueue<Integer, String> queue) {
        super(queue, 5, 100);
    }

    protected void executeBatch(final List<BatchElement<Integer, String>> batch) {
        String suffix = " with " + (batch.size()-1) + " other elements";
        for (BatchElement<Integer, String> element : batch) {
            element.success("Batched " + element.getInputValue() + suffix);
        }
    }
}
````

