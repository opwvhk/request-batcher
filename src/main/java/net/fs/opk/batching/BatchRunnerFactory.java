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


/**
 * A factory class to create various variations of batch runners.
 */
public final class BatchRunnerFactory {
	/**
	 * <p>Create a batch runner for a batch consumer (starter), to run potentially infinite batches simultaneously.</p>
	 *
	 * <p>If your batch consumer blocks until the batch completes, there will only be one batch executed at a time.</p>
	 *
	 * @param queue        the queue to read batches off of
	 * @param batchSize    the maximum size of a batch
	 * @param batchStarter a batch consumer
	 * @return the resulting batch runner
	 */
	public static <Request, Response> BatchRunner<Request, Response> forConsumer(BatchQueue<Request, Response> queue, int batchSize,
	                                                                             Consumer<List<BatchElement<Request, Response>>> batchStarter) {
		return new BatchRunner<>(queue, batchSize) {
			@Override
			protected void startRequest(List<BatchElement<Request, Response>> batch) {
				batchStarter.accept(batch);
			}
		};
	}


	/**
	 * <p>Create a batch runner for a batch consumer (starter), to run a limited number of batches simultaneously.</p>
	 *
	 * <p>Your batch consumer must not block, or else there will only be one batch executed at a time. Also note that {@code batchTimeout} does not cancel the
	 * batch: it only times out the batch elements and allows a new batch to start. If needed, you must implement a cancellation mechanism in
	 * {@code batchStarter}.
	 *
	 * @param queue                the queue to read batches off of
	 * @param batchSize            the maximum size of a batch
	 * @param maxConcurrentBatches the maximum number of batches that may run concurrently
	 * @param batchTimeout         the maximum time a batch may run, to prevent resource starvation
	 * @param unit                 the unit for {@code batchTimeout}
	 * @param batchStarter         a batch consumer
	 * @return the resulting batch runner
	 */
	public static <Request, Response> BatchRunner<Request, Response> forConsumer(BatchQueue<Request, Response> queue, int batchSize,
	                                                                             int maxConcurrentBatches, long batchTimeout, TimeUnit unit,
	                                                                             Consumer<List<BatchElement<Request, Response>>> batchStarter) {
		Semaphore semaphore = new Semaphore(maxConcurrentBatches, true);

		return new BatchRunner<>(queue, batchSize) {
			@Override
			protected void preBatch() throws InterruptedException {
				semaphore.acquire();
			}


			@Override
			protected void startRequest(List<BatchElement<Request, Response>> batch) {
				if (batch.isEmpty()) {
					semaphore.release();
				} else {
					AtomicInteger counter = new AtomicInteger(batch.size());
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
	 * <p>Create a batch consumer from explicit (de)muxers and a batched function call.</p>
	 *
	 * <p>Both {@code muxer} and {@code batchFunction} run on the {@link BatchRunner BatchRunner&lt;Request,Response>} thread, and {@code demuxer} runs on the
	 * same thread that completes the {@code batchFunction}s result.</p>
	 *
	 * <p>The returned consumer guarantees that all batch elements will complete: if {@code muxer} or {@code batchFunction} throw an exception or if
	 * {@code demuxer} does not complete them before returning, it will fail them.</p>
	 *
	 * @param muxer         a function that multiplexes a list of input values into a singe batch request
	 * @param batchFunction a function to execute batch requests
	 * @param demuxer       a consumer that (synchronously) demultiplexes a batch response into all batch elements
	 */
	public static <Request, BatchedRequest, BatchedResponse, Response> Consumer<List<BatchElement<Request, Response>>> multiplexOverFunction(
		Function<List<Request>, BatchedRequest> muxer, Function<BatchedRequest, CompletableFuture<BatchedResponse>> batchFunction,
		BiConsumer<BatchedResponse, List<BatchElement<Request, Response>>> demuxer) {
		return batch -> {
			List<Request> values = new ArrayList<>(batch.size());
			for (BatchElement<Request, Response> element : batch) {
				values.add(element.getInputValue());
			}

			batchFunction.apply(muxer.apply(values))
				.thenAccept(batchResponse -> demuxer.accept(batchResponse, batch))
				.whenComplete((_ignored, error) -> {
					// Ideally, the call succeeded and the filter yields no results.
					// If not, or when the demuxer doesn't complete all calls, fail the remaining calls.
					Throwable failure = null;
					for (BatchElement<Request, Response> element : batch) {
						if (!element.outputFuture.isDone()) {
							if (failure == null) {
								failure = error != null ? error : new IllegalStateException("The batch call didn't yield a value...");
							}
							element.error(failure);
						}
					}
				});
		};
	}

	private BatchRunnerFactory() {
		// Utility class: do not instantiate.
	}
}
