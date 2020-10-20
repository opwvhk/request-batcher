package net.fs.opk.batching;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Class to start batched requests off a {@link BatchQueue}. Subclasses must at least implement
 * {@link #startRequest(List) startRequest(List&lt;BatchElement&lt;Request, Response&gt;&gt;)}, but may also override {@link #preBatch()}.
 *
 * <p>Implementation note: when running, the call sequence is always</p><ol>
 *
 * <li>{@code preBatch()}</li>
 *
 * <li>acquire a batch from the queue</li>
 *
 * <li>{@code startRequest(List<BatchElement<Request, Response>>)} (with an empty list if the queue was empty)</li>
 *
 * </ol>
 */
public abstract class BatchRunner<Request, Response> implements Runnable {
	/**
	 * Logger for this class.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(BatchRunner.class);

	private final BatchQueue<Request, Response> queue;
	private final int batchSize;
	private final long batchTimeout;
	private final TimeUnit batchTimeoutUnit;


	/**
	 * Create a batch runner that reads off a queue, using a 5 second timeout when acquiring a batch.
	 *
	 * @param queue     the batch queue to read
	 * @param batchSize the maximum number of elements in a batch
	 * @see BatchQueue#acquireBatch(long, TimeUnit, int, java.util.Collection)
	 * BatchQueue.acquireBatch(long, TimeUnit, int, Collection&lt;BatchElement&lt;Request, Response&gt;&gt;)
	 */
	protected BatchRunner(final BatchQueue<Request, Response> queue, final int batchSize) {
		this(queue, batchSize, 5, TimeUnit.SECONDS);
	}


	/**
	 * Create a batch runner that reads off a queue.
	 *
	 * @param queue            the batch queue to read
	 * @param batchSize        the maximum number of elements in a batch
	 * @param batchTimeout     the timeout to use when acquiring a batch of elements from the queue
	 * @param batchTimeoutUnit the unit of the timeout
	 * @see BatchQueue#acquireBatch(long, TimeUnit, int, java.util.Collection)
	 * BatchQueue.acquireBatch(long, TimeUnit, int, Collection&lt;BatchElement&lt;Request, Response&gt;&gt;)
	 */
	protected BatchRunner(final BatchQueue<Request, Response> queue, final int batchSize, final long batchTimeout, final TimeUnit batchTimeoutUnit) {
		this.queue = queue;
		this.batchSize = batchSize;
		this.batchTimeout = batchTimeout;
		this.batchTimeoutUnit = batchTimeoutUnit;
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
				continueRunning = tryBatchRequest();
			}
			LOGGER.info("BatchRunner stopping because queue is shutdown and empty.");
		} catch (final InterruptedException ignored) {
			LOGGER.info("BatchRunner stopping because its thread was interrupted.");
		} catch (final Throwable error) {
			LOGGER.error("BatchRunner stopping abnormally", error);
		}
	}


	/**
	 * Try a single batch request. This method waits a short time for a batch to fill, and then up to the specified linger time for more
	 * elements (less if the first element was already waiting when this method started). Then calls
	 * {@link #startRequest(List) startRequest(List&lt;BatchElement&lt;Request, Response&gt;&gt;)}.
	 *
	 * @return {@code true} if this method should be called again, {@code false} if the queue has shut down and is empty.
	 * @throws InterruptedException if this thread was interrupted while waiting
	 */
	private boolean tryBatchRequest() throws Exception {
		preBatch();

		final List<BatchElement<Request, Response>> batch = new ArrayList<>(batchSize);
		final boolean keepRunning = queue.acquireBatch(batchTimeout, batchTimeoutUnit, batchSize, batch);
		try {
			startRequest(batch);
		} catch (final Throwable error) {
			batch.forEach(element -> element.error(error));
		}
		return keepRunning;
	}


	/**
	 * Method that is called every time just before polling the queue for a new batch of elements.
	 *
	 * <p>After every call to this method {@link #startRequest(List) startRequest(List&lt;BatchElement&lt;Request, Response&gt;&gt;)} is
	 * called.</p>
	 *
	 * <p><strong>Note:</strong> if this method throws, the batch runner shuts down.</p>
	 */
	protected void preBatch() throws Exception {
		// Default implementation: do nothing
	}


	/**
	 * Start to execute a batch of elements, reporting their results eventually.
	 *
	 * <p>This method is called for every batch, after {@link #preBatch()} is called. If the queue was empty, the list will be empty.</p>
	 *
	 * <p>Note: if this method throws, the exception is passed to the batch elements and the batch runner keeps running.</p>
	 *
	 * @param batch the batch of elements
	 */
	protected abstract void startRequest(final List<BatchElement<Request, Response>> batch) throws Exception;
}
