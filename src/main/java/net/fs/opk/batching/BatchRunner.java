package net.fs.opk.batching;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to execute batched requests off a {@link BatchQueue}. To use, either implement
 * {@link #executeBatch(List) executeBatch(List&lt;BatchElement&lt;Request, Response&gt;&gt;)} or use the static method
 * {@link #forLambdas(BatchQueue, int, long, Function, Function, BiConsumer)}.
 */
public abstract class BatchRunner<Request, Response> implements Runnable {
	/**
	 * Logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(BatchRunner.class);

	private final BatchQueue<Request, Response> queue;
	private final int batchSize;
	private final long batchTimeoutMs;


	/**
	 * Create a {@code BatchRunner} for the specified queue, using explicit (de)muxers and a batched function call.
	 *
	 * @param queue          the batch quueue to use
	 * @param batchSize      the maximum number of elemnts to
	 * @param batchTimeoutMs the maximum tiime to wait for the {@code batchFunction} and {@code demuxer} to yield results
	 * @param muxer          a function that multiplexes a list of inputs into a singe batch request
	 * @param batchFunction  a function to execute batch requests
	 * @param demuxer        a consumer that demultiplexes a batch response into the batch elements
	 */
	public static <Request, BatchedRequest, BatchedResponse, Response> BatchRunner<Request, Response> forLambdas(final BatchQueue<Request, Response> queue,
			final int batchSize, final long batchTimeoutMs, final Function<List<Request>, BatchedRequest> muxer,
			final Function<BatchedRequest, CompletableFuture<BatchedResponse>> batchFunction,
			final BiConsumer<BatchedResponse, List<BatchElement<Request, Response>>> demuxer) {
		return new BatchRunner<Request, Response>(queue, batchSize, batchTimeoutMs) {
			@Override
			protected void executeBatch(final List<BatchElement<Request, Response>> batch) {
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
			}
		};
	}


	/**
	 * Create a batch runner that reads off a queue.
	 *
	 * @param queue          the batch queue to read
	 * @param batchSize      the maximum number of elements in a bach
	 * @param batchTimeoutMs the maximum time (in ms) to wait for the batch to produce results after
	 *                       {@link #executeBatch(List) executeBatch(List&lt;BatchElement&lt;Request, Response&gt;&gt;)} has completed
	 */
	protected BatchRunner(final BatchQueue<Request, Response> queue, final int batchSize, final long batchTimeoutMs) {
		this.queue = queue;
		this.batchSize = batchSize;
		this.batchTimeoutMs = batchTimeoutMs;
	}


	/**
	 * Repeatedly get a batch from the queue and execute it. Stops when the queue has shut down and is empty or when the
	 * current thread is interrupted.
	 */
	@Override
	public void run() {
		LOGGER.info("BatchRunner starting.");
		try {
			boolean continueRunning = true;
			while (continueRunning) {
				continueRunning = tryBatchRun();
			}
			LOGGER.info("BatchRunner stopping because queue is shutdown and empty.");
		} catch (final InterruptedException ignored) {
			LOGGER.info("BatchRunner stopping because its thread was interrupted.");
		} catch (final Throwable error) {
			LOGGER.error("BatchRunner stopping abnormally", error);
			throw error;
		}
	}


	/**
	 * Try a single batch call. This method waits a short time for a batch to fill, and then up to the specified linger time for more elements. Then, if there
	 * are any, calls {@link #executeBatch(List) executeBatch(List&lt;BatchElement&lt;Request, Response&gt;&gt;)}.
	 *
	 * @return {@code true} if this method should be called again, {@code false} if the queue has shut down and is empty.
	 * @throws InterruptedException if this thread was interrupted while waiting
	 */
	private boolean tryBatchRun() throws InterruptedException {
		final List<BatchElement<Request, Response>> batch = new ArrayList<>(batchSize);
		final boolean keepRunning = queue.acquireBatch(100, TimeUnit.MILLISECONDS, batchSize, batch);
		if (batch.isEmpty()) {
			return keepRunning;
		}

		try {
			final CountDownLatch latch = new CountDownLatch(batch.size());
			final BiConsumer<Response, Throwable> latchCountDown = (ignoredResult, ignoredError) -> latch.countDown();
			batch.forEach(element -> element.outputFuture.whenComplete(latchCountDown));

			executeBatch(batch);

			// Ensure we don't run multiple requests at the same time.
			if (!latch.await(batchTimeoutMs, TimeUnit.MILLISECONDS)) {
				throw new TimeoutException("The batched request did not complete on time.");
			}
		} catch (final Throwable error) {
			batch.forEach(element -> element.error(error));
		}

		return keepRunning;
	}


	/**
	 * Execute a batch of elements, reporting their results eventually (or at least within the timeout specified in the constuctor).
	 *
	 * @param batch the batch of elements
	 */
	protected abstract void executeBatch(final List<BatchElement<Request, Response>> batch);
}
