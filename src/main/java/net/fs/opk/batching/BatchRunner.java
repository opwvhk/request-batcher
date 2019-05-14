package net.fs.opk.batching;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Class to start batched requests off a {@link BatchQueue}.
 *
 * <p>Implementation notes:</p><ul>
 *
 * <li>Subclasses must at least implement {@link #startBatch(List) startBatch(List&lt;BatchElement&lt;Request, Response&gt;&gt;)}</li>
 *
 * <li>When running, the call sequence is always:</li>
 *
 * <li></li>
 *
 * <li></li>
 *
 * <li></li>
 *
 * </ul>
 */
public abstract class BatchRunner<Request, Response> implements Runnable {
	/**
	 * Logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(BatchRunner.class);

	private final BatchQueue<Request, Response> queue;
	private final int batchSize;


	/**
	 * Create a batch runner that reads off a queue.
	 *
	 * @param queue     the batch queue to read
	 * @param batchSize the maximum number of elements in a batch
	 */
	protected BatchRunner(final BatchQueue<Request, Response> queue, final int batchSize) {
		this.queue = queue;
		this.batchSize = batchSize;
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
		}
	}


	/**
	 * Try a single batch call. This method waits a short time for a batch to fill, and then up to the specified linger time for more elements (less if the
	 * first element was already waiting when this method started). Then, if there are any elements, calls
	 * {@link #startBatch(List) startBatch(List&lt;BatchElement&lt;Request, Response&gt;&gt;)}.
	 *
	 * @return {@code true} if this method should be called again, {@code false} if the queue has shut down and is empty.
	 * @throws InterruptedException if this thread was interrupted while waiting
	 */
	private boolean tryBatchRun() throws Exception {
		preBatch();

		final List<BatchElement<Request, Response>> batch = new ArrayList<>(batchSize);
		final boolean keepRunning = queue.acquireBatch(100, TimeUnit.MILLISECONDS, batchSize, batch);
		if (batch.isEmpty()) {
			noBatch();
		} else {
			try {
				startBatch(batch);
			} catch (final Throwable error) {
				batch.forEach(element -> element.error(error));
			}
		}
		return keepRunning;
	}


	/**
	 * Method that is called every time just before polling the queue for a new batch of elements.
	 *
	 * <p>After every call to this method, either {@link #noBatch()} or {@link #startBatch(List) startBatch(List&lt;BatchElement&lt;Request, Response&gt;&gt;)}
	 * is called.</p>
	 *
	 * <p><strong>Note:</strong> if this method throws, the batch runner shuts down.</p>
	 */
	protected void preBatch() throws Exception {
		// Default implementation: do nothing
	}


	/**
	 * Start to execute a batch of elements, reporting their results eventually (or at least within the timeout specified in the constuctor).
	 *
	 * <p>This method is called for every batch, after {@link #preBatch()} is called. If the queue was empty, {@link #noBatch()} is called instead.</p>
	 *
	 * <p>Note: if this method throws, the exception is pased to the batch elements and the batch runner keeps running.</p>
	 *
	 * @param batch the batch of elements
	 */
	protected abstract void startBatch(final List<BatchElement<Request, Response>> batch) throws Exception;


	/**
	 * Method that is called every time the queue is empty.
	 *
	 * <p><strong>Note:</strong> if this method throws, the batch runner shuts down.</p>
	 */
	protected void noBatch() throws Exception {
		// Default implementation: do nothing
	}
}
