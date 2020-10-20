package net.fs.opk.batching;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * A factory class to create various variations of batch runners.
 */
public class BatchRunnerFactory {
	/**
	 * Create a batch runner for a batch consumer (starter), to run potentially infinite batches simultaneously. If your batch consumer blocks until the batch
	 * is handled, there will only be one batch executed at a time.
	 *
	 * @param queue        the queue to read batches off of
	 * @param batchSize    the maximum size of a batch
	 * @param batchStarter a batch consumer
	 * @return the resulting batch runner
	 */
	public static <Request, Response> BatchRunner<Request, Response> forConsumer(final BatchQueue<Request, Response> queue, final int batchSize,
	                                                                             Consumer<List<BatchElement<Request, Response>>> batchStarter) {
		return new BatchRunner<>(queue, batchSize) {
			@Override
			protected void startRequest(final List<BatchElement<Request, Response>> batch) {
				batchStarter.accept(batch);
			}
		};
	}


	/**
	 * Create a batch runner for a batch consumer (starter), to run a limited number of batches simultaneously. Your batch consumer must not block, or else
	 * there will only be one batch executed at a time. Also note that {@code batchTimeout} does not cancel the batch: it only times out the batch elements and
	 * allows a new batch to start.
	 *
	 * @param queue                the queue to read batches off of
	 * @param batchSize            the maximum size of a batch
	 * @param maxConcurrentBatches the maximum number of batches that may run concurrently
	 * @param batchTimeout         the maximum time a batch may run, to prevent resource starvation
	 * @param unit                 the unit for {@code batchTimeout}
	 * @param batchStarter         a batch consumer
	 * @return the resulting batch runner
	 */
	public static <Request, Response> BatchRunner<Request, Response> forConsumer(final BatchQueue<Request, Response> queue, final int batchSize,
	                                                                             final int maxConcurrentBatches, final long batchTimeout, final TimeUnit unit,
	                                                                             Consumer<List<BatchElement<Request, Response>>> batchStarter) {
		final Semaphore semaphore = new Semaphore(maxConcurrentBatches, true);

		return new BatchRunner<>(queue, batchSize) {
			@Override
			protected void preBatch() throws InterruptedException {
				semaphore.acquire();
			}


			@Override
			protected void startRequest(final List<BatchElement<Request, Response>> batch) {
				if (batch.isEmpty()) {
					semaphore.release();
				} else {
					final AtomicInteger counter = new AtomicInteger(batch.size());
					batch.forEach(element -> element.outputFuture.orTimeout(batchTimeout, unit).whenComplete((ignored1, ignored2) -> {
						if (counter.decrementAndGet() == 0) {
							semaphore.release();
						}
					}));
					batchStarter.accept(batch);
				}
			}
		};
	}


	/**
	 * Create a batch consumer from explicit (de)muxers and a batched function call.
	 *
	 * @param muxer         a function that multiplexes a list of inputs into a singe batch request
	 * @param batchFunction a function to execute batch requests
	 * @param demuxer       a consumer that demultiplexes a batch response into the batch elements
	 */
	public static <Request, BatchedRequest, BatchedResponse, Response> Consumer<List<BatchElement<Request, Response>>> multiplexOverFunction(
		final Function<List<Request>, BatchedRequest> muxer, final Function<BatchedRequest, CompletableFuture<BatchedResponse>> batchFunction,
		final BiConsumer<BatchedResponse, List<BatchElement<Request, Response>>> demuxer) {
		return batch -> {
			final List<Request> values = new ArrayList<>(batch.size());
			for (final BatchElement<Request, Response> element : batch) {
				values.add(element.getInputValue());
			}

			CompletableFuture.completedFuture(values).thenApply(muxer).thenCompose(batchFunction).thenAccept(batchedResponse -> {
				demuxer.accept(batchedResponse, batch);

				// Just in case the demuxer didn't complete all requests.
				final List<BatchElement<Request, Response>> unhandledElements =
					batch.stream().filter(element -> !element.outputFuture.isDone()).collect(Collectors.toList());
				if (!unhandledElements.isEmpty()) {
					final Exception error = new IllegalStateException("The batch call didn't yield a value...");
					for (final BatchElement<Request, Response> element : unhandledElements) {
						element.error(error);
					}
				}
			}).exceptionally(error -> {
				batch.forEach(element -> element.error(error));
				return null;
			});
		};
	}
}
