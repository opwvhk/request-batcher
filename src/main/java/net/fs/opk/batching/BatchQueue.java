package net.fs.opk.batching;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import net.fs.opk.util.Namer;

import static java.util.Objects.requireNonNull;

/**
 * A capacity limited queue to submit calls to that will be executed in batches. Its main use is for a {@link BatchRunner} running in a separate thread. As this
 * is a multithreaded process, this queue can be shutdown after which no new elements can be added. Shutdown is complete when the queue has been shutdown and
 * emptied.
 *
 * <p>This queue does not implement {@link BlockingQueue} nor {@link Queue}, because it's not intended to be used as a {@link Collection}, but rather as a
 * bidirectional channel. This is also why {@link #enqueue(Object)} and {@link #enqueue(Object, long, TimeUnit)} return a {@link CompletableFuture}.</p>
 *
 * <h2>Throughput, latency and order</h2>
 *
 * <p>In addition to method specific timeouts, elements on the queue also have a 'timeout': the linger time (specified when constructing the queue). This
 * linger time specifies the amount of time an item on the queue is willing to wait for a batch to fill. When it expires, the item will be picked up in a batch
 * as soon as possible. Larger linger times directly affect latency (it's the minimum latency for the first item in a batch) and increase the probability to
 * batch multiple elements together, thus indirectly increasing throughput.</p>
 *
 * <p>The order of items on the queue is determined by the order the elements are added. This order is kept when consuming batches off the queue using a single
 * thread, but not when consuming the queue with multiple threads: if the queue contains elements [A, B, C, D, E], two threads simultaneously acquiring a batch
 * may receive the batches [A, C, D] and [B, E] respectively.</p>
 */
public class BatchQueue<Request, Response> {
	private static final Namer metricNamer = new Namer("batch_queue_");
	/**
	 * The queued items.
	 * <p>
	 * (visible for testing because invariants can be tricky)
	 */
	final BatchElement<Request, Response>[] items;
	/**
	 * The index for the next enqueue action.
	 * <p>
	 * (visible for testing because invariants can be tricky)
	 */
	int enqueueIndex;
	/**
	 * The index for the next dequeue action.
	 * <p>
	 * (visible for testing because invariants can be tricky)
	 */
	int dequeueIndex;
	/**
	 * The number of elements in the queue.
	 * <p>
	 * (visible for testing because invariants can be tricky)
	 */
	int count;
	/**
	 * Whether we can accept new elements.
	 */
	private boolean isShutdown;

	/*
	 * Concurrency control uses the classic two-condition algorithm found in any textbook.
	 */

	/**
	 * Main lock guarding all access.
	 */
	private final ReentrantLock lock;
	/**
	 * Condition to await free capacity. Both decrementing {@code count} and setting {@code isShutdown} to {@code true} signals this condition.
	 */
	private final Condition elementRemoved;
	/**
	 * Condition to await new elements. Both incrementing {@code count} and setting {@code isShutdown} to {@code true} signals this condition.
	 */
	private final Condition elementAdded;

	/*
	 * Other fields.
	 */

	/**
	 * The number of nanoseconds an element may linger to await more elements. Elements will be on the queue at least this long, except when the batch they end
	 * up in is full sooner.
	 */
	private final long lingerNs;
	/**
	 * The timeout (in ns) for enqueued elements to complete. This is the timeout on the queue plus all processing until the final result.
	 */
	private final long timeoutNs;
	private final Timer queueTimer;
	private final Timer processingTimer;

	/**
	 * Create a batch queue with the given capacity.
	 *
	 * @param capacity    the maximum number of items on the queue
	 * @param linger      the time an item may wait on the queue to enlarge the batch
	 * @param lingerUnit  the unit of the parameter {@code linger}
	 * @param timeout     the maximum time until enqueued items must have a result
	 * @param timeoutUnit the unit of the parameter {@code timeout}
	 */
	public BatchQueue(int capacity, long linger, TimeUnit lingerUnit, long timeout, TimeUnit timeoutUnit) {
		this(capacity, linger, lingerUnit, timeout, timeoutUnit, null);
	}

	/**
	 * Create a batch queue with the given capacity.
	 *
	 * @param capacity    the maximum number of items on the queue
	 * @param linger      the time an item may wait on the queue to enlarge the batch
	 * @param lingerUnit  the unit of the parameter {@code linger}
	 * @param timeout     the maximum time until enqueued items must have a result
	 * @param timeoutUnit the unit of the parameter {@code timeout}
	 */
	public BatchQueue(int capacity, long linger, TimeUnit lingerUnit, long timeout, TimeUnit timeoutUnit, String queueName, String... queueTags) {
		if (capacity <= 0) {
			throw new IllegalArgumentException("Capacity must be positive");
		}
		lingerNs = lingerUnit.toNanos(linger);
		if (lingerNs < 0 || lingerNs == Long.MAX_VALUE) {
			throw new IllegalArgumentException("The linger time must be non-negative and less than 2^63-1 ns (approx. 292 years)");
		}
		timeoutNs = timeoutUnit.toNanos(timeout);
		if (timeoutNs <= lingerNs || timeoutNs == Long.MAX_VALUE) {
			throw new IllegalArgumentException("The timeout must be larger than the linger time and less than 2^63-1 ns (approx. 292 years)");
		}

		String metricName = metricNamer.name(queueName);
		queueTimer = Metrics.timer(metricName + ".queued", queueTags);
		processingTimer = Metrics.timer(metricName + ".processing", queueTags);

		items = (BatchElement<Request, Response>[]) new BatchElement[capacity];
		enqueueIndex = 0;
		dequeueIndex = 0;
		count = 0;

		lock = new ReentrantLock();
		elementRemoved = lock.newCondition();
		elementAdded = lock.newCondition();
	}

	/**
	 * Enqueue an item onto this batch queue. Callers must acquire the lock, and ensure that the queue is not full.
	 *
	 * @param item the item to enqueue
	 * @return the result future for the enqueued item
	 */
	private CompletableFuture<Response> enqueue0(Request item) {
		// Assumes we hold the lock and count < items.length (and thus items[enqueueIndex] == null)

		// Calculate deadlines here:
		long nanoTime = System.nanoTime();
		BatchElement<Request, Response> element = new BatchElement<>(item, nanoTime + lingerNs, timeoutNs);
		element.outputFuture.whenComplete((ignored1, ignored2) -> processingTimer.record(System.nanoTime() - nanoTime, TimeUnit.NANOSECONDS));
		items[enqueueIndex] = element;

		enqueueIndex++;
		if (enqueueIndex == items.length) {
			enqueueIndex = 0;
		}
		count++;
		elementAdded.signal();

		return element.outputFuture;
	}

	/**
	 * Dequeue a batched element from this batch queue. Callers must acquire the lock, and ensure that the queue is not empty.
	 *
	 * @return the dequeued batched element
	 */
	private BatchElement<Request, Response> dequeue0() {
		// Assumes we hold the lock and count > 0 (and thus items[dequeueIndex] != null)

		BatchElement<Request, Response> element = items[dequeueIndex];
		items[dequeueIndex] = null;

		dequeueIndex++;
		if (dequeueIndex == items.length) {
			dequeueIndex = 0;
		}
		count--;
		elementRemoved.signal();
		return element;
	}

	/**
	 * Enqueue a request to execute, if it can be done immediately.
	 *
	 * @param request the request to add to the queue
	 * @return a future response if successful, or {@code null} if the queue was full
	 */
	public CompletableFuture<Response> enqueue(Request request) {
		requireNonNull(request);
		CompletableFuture<Response> future;

		lock.lock();
		try {
			if (isShutdown || count == items.length) {
				return null;
			}
			future = enqueue0(request);
		} finally {
			lock.unlock();
		}
		return future;
	}

	/**
	 * Enqueue a request to execute, waiting up to the specified timeout if the queue is full.
	 *
	 * @param request the request to add to the queue
	 * @param timeout the maximum time to wait for queue capacity; must be non-negative
	 * @param unit    the unit of the parameter {@code timeout}
	 * @return a future response if successful, or {@code null} if the timeout elapsed before there was a place on the batch queue
	 * @throws InterruptedException if this thread was interrupted while waiting
	 */
	public CompletableFuture<Response> enqueue(Request request, long timeout, TimeUnit unit) throws InterruptedException {
		requireNonNull(request);
		if (timeout < 0) {
			throw new IllegalArgumentException("timeout must be non-negative");
		}
		long nanosToTimeout = unit.toNanos(timeout);
		CompletableFuture<Response> future;

		lock.lockInterruptibly();
		try {
			while (!isShutdown && count == items.length) {
				if (nanosToTimeout <= 0) {
					return null;
				}
				nanosToTimeout = elementRemoved.awaitNanos(nanosToTimeout);
			}
			if (isShutdown) {
				return null;
			}
			future = enqueue0(request);
		} finally {
			lock.unlock();
		}
		return future;
	}

	/**
	 * Shutdown the batch queue. After this method has been called, no new elements can be added and {@link #acquireBatch(long, TimeUnit, int, Collection)} will
	 * return {@code false} eventually (i.e., when the underlying queue is empty).
	 */
	public void shutdown() {
		lock.lock();
		try {
			isShutdown = true;
			// Signal all waiting threads: as the queue can only be emptied now, there's no need to wait for capacity nor wait for new elements.
			elementRemoved.signal();
			elementAdded.signal();
		} finally {
			lock.unlock();
		}
		Metrics.globalRegistry.remove(queueTimer);
		Metrics.globalRegistry.remove(processingTimer);
	}

	/**
	 * Check whether the batch queue has been shutdown. If so, calls to {@link #enqueue(Object)} and {@link #enqueue(Object, long, TimeUnit)} will always return
	 * {@code null}.
	 *
	 * @return {@code true} if the queue has been shutdown, {@code true} otherwise
	 */
	public boolean isShutdown() {
		boolean result;
		lock.lock();
		try {
			result = this.isShutdown;
		} finally {
			lock.unlock();
		}
		return result;
	}

	/**
	 * Check whether the batch queue has been shutdown and emptied. If so, calls to {@link #enqueue(Object)} and {@link #enqueue(Object, long, TimeUnit)} will
	 * always return {@code null}, and calls to {@link #acquireBatch(long, TimeUnit, int, Collection)} will always return {@code false}.
	 *
	 * @return {@code true} if the queue has been shutdown and emptied, {@code true} otherwise
	 */
	public boolean isShutdownComplete() {
		boolean result;
		lock.lock();
		try {
			result = isShutdown && count == 0;
		} finally {
			lock.unlock();
		}
		return result;
	}

	/**
	 * Wait up to the specified timeout for {@link #isShutdownComplete()} to return {@code true}.
	 *
	 * @param timeout the maximum time to wait; must be non-negative
	 * @param unit    the unit of the parameter {@code timeout}
	 * @return {@code true} if the queue was shutdown and emptied within the timeout, {@code false} if not
	 * @throws InterruptedException if this thread was interrupted while waiting
	 */
	public boolean awaitShutdownComplete(long timeout, TimeUnit unit) throws InterruptedException {
		if (timeout < 0) {
			throw new IllegalArgumentException("timeout must be non-negative");
		}
		long nanosToTimeout = unit.toNanos(timeout);

		lock.lock();
		try {
			boolean result;
			while (!(result = isShutdown && count == 0) && nanosToTimeout > 0) {
				// dequeue0 signals elementRemoved
				nanosToTimeout = elementRemoved.awaitNanos(nanosToTimeout);
			}
			return result;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Acquire a batch of items from the queue. Waits up to the specified timeout for items to become available, and up to the specified linger time for a
	 * second item to become available. It returns after that, after having placed at most {@code maxElements} elements in the specified collection.
	 * <p>
	 * This means that this method lingers for at most {@code linger} {@code unit}s after it has acquired an item from the queue.
	 *
	 * @param timeout     the maximum amount of time to wait for a first item to become available; must be non-negative
	 * @param unit        the unit of the {@code timeout} parameter
	 * @param maxElements the maximum number of elements to put into {@code collection}
	 * @param collection  the collection to add the elements to
	 * @return {@code true} until both {@link #shutdown()} has been called and the underlying queue is empty, {@code false} after that
	 * @throws InterruptedException when the current thread was interrupted while waiting
	 */
	public boolean acquireBatch(long timeout, TimeUnit unit, int maxElements, Collection<BatchElement<Request, Response>> collection)
		throws InterruptedException {
		if (timeout < 0) {
			throw new IllegalArgumentException("timeout must be non-negative");
		}
		if (maxElements <= 0) {
			throw new IllegalArgumentException("maxElements must be positive");
		}
		requireNonNull(collection, "You must supply a collection");

		boolean canYieldMoreBatches;

		lock.lockInterruptibly();
		try {
			int elementsInBatch = 0;
			long nanosToTimeout = unit.toNanos(timeout);
			while (elementsInBatch < maxElements && (count > 0 || !isShutdown && nanosToTimeout >= 0)) {
				while (!isShutdown && count == 0 && nanosToTimeout > 0) {
					nanosToTimeout = elementAdded.awaitNanos(nanosToTimeout);
				}
				if (count > 0) {
					// Using dequeue0() because it preserves invariants
					BatchElement<Request, Response> element = dequeue0();
					if (element.outputFuture.isDone()) {
						continue; // Already done (probably cancelled); adding it to the batch makes no sense anymore
					}
					long lingerTimeLeft = element.lingerDeadlineNanos - System.nanoTime(); // Can become negative (by design)
					long nanosInQueue = lingerNs - lingerTimeLeft;
					queueTimer.record(nanosInQueue, TimeUnit.NANOSECONDS);
					if (elementsInBatch == 0) {
						// This is the first element in the batch, which will have the first expiring linger time on the queue.
						// We update our remaining time (if needed) to honor that.
						nanosToTimeout = Math.min(nanosToTimeout, lingerTimeLeft);
					}

					elementsInBatch++;
					collection.add(element);
				}
			}

			canYieldMoreBatches = count > 0 || !isShutdown;
		} finally {
			lock.unlock();
		}
		return canYieldMoreBatches;
	}
}
