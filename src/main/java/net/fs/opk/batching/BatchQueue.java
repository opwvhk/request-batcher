package net.fs.opk.batching;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;


/**
 * A capacity limited queue to submit calls to that will be executed in batches. The queue is assumed to be consumed by one or more {@link BatchRunner}
 * instances running in separate threads. As this is a multithreaded process, this queue can be shutdown after which no new elements can be added. Shutdown
 * is considered complete when the queue has been shutdown and emptied.
 *
 * <h2>Throughput, latency and order</h2>
 *
 * <p>In addition to method specific timeouts, elements on the queue also have a 'timeout': the linger time (specified when constructing the queue). This linger
 * time specifies the amount of time an item on the queue is willing to wait for a batch to fill. When it expires, the item will be picked up in a batch as
 * soon as possible. Larger linger times directly affect latency (it's the minimum latency for the first item in a batch) and increase the probability to
 * batch multiple elements together, thus indirectly increasing throughput.</p>
 *
 * <p>The order of items on the queue is determined by the order the elements are added. This order is kept when consuming batches off the queue in a single
 * thread, but not when consuming the queue with multiple threads: if the queue contains elements [A, B, C, D, E], two threads simultaniously acquiring a batch
 * may receive the batches [A, C, D] and [B, E] respectively. To preserve this order, use one thread to consume batches and another to handle them.</p>
 *
 * <p>This queue does not implement {@link BlockingQueue} nor {@link Queue}, because it's not intended to be used as a {@link Collection}, but rather as a
 * bidirectional channel. This is also why {@link #enqueue(Object)} and {@link #enqueue(Object, long, TimeUnit)} return a {@link CompletableFuture}.</p>
 */
public class BatchQueue<Request, Response> {
	/**
	 * The queued items.
	 * <p>
	 * (visible for testing because invariants can be tricky)
	 */
	final BatchElement[] items;
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
	 * Condition to await free capacity. This condition is signalled on both decrementing {@code count} and setting {@code isShutdown} to {@code true}.
	 */
	private final Condition elementRemoved;
	/**
	 * Condition to await new elements. This condition is signalled on both incrementing {@code count} and setting {@code isShutdown} to {@code true}.
	 */
	private final Condition elementAdded;

	/*
	 * Other fields.
	 */

	/**
	 * The number of nanoseconds an element may linger on the queue to await more elements. Elements will be on the queue at least this long, except when the
	 * batch they end up in is full sooner.
	 */
	private final long lingerNs;


	/**
	 * Create a batch queue with the given capacity.
	 *
	 * @param capacity the maximum number of items on the queue
	 * @param linger   the time an item may wait on the queue to enlarge the batch
	 * @param unit     the unit of the parameter {@code timeout}
	 */
	public BatchQueue(final int capacity, final long linger, final TimeUnit unit) {
		if (capacity <= 0) {
			throw new IllegalArgumentException("Capacity must be positive");
		}
		if (unit.toNanos(linger) < 0 || unit.toNanos(linger) == Long.MAX_VALUE) {
			throw new IllegalArgumentException("The linger time must be non-negative and less than 2^63 ns (approx. 292 years)");
		}
		items = new BatchElement[capacity];
		enqueueIndex = 0;
		dequeueIndex = 0;
		count = 0;

		lock = new ReentrantLock();
		elementRemoved = lock.newCondition();
		elementAdded = lock.newCondition();

		lingerNs = unit.toNanos(linger);
	}


	/**
	 * Enqueue an item onto this batch queue. Assumes this thread holds the lock, and that count < items.length (and thus that items[enqueueIndex] == null).
	 *
	 * @param item the item to enqueue
	 * @return the result future for the enqueued item
	 */
	private CompletableFuture<Response> enqueue0(final Request item) {
		// Assumes we hold the lock and count < items.length (and thus that items[enqueueIndex] == null)

		final BatchElement<Request, Response> element = new BatchElement<>(System.nanoTime() + lingerNs, item);
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
	 * Dequeue a batched element from this batch queue. Assumes this thread holds the lock, and that count > 0 (and thus that items[dequeueIndex] != null).
	 *
	 * @return the dequeued batched element
	 */
	private BatchElement<Request, Response> dequeue0() {
		// Assumes we hold the lock and count > 0 (and thus that items[dequeueIndex] != null)

		final BatchElement<Request, Response> x = (BatchElement<Request, Response>)items[dequeueIndex];
		items[dequeueIndex] = null;

		dequeueIndex++;
		if (dequeueIndex == items.length) {
			dequeueIndex = 0;
		}
		count--;
		elementRemoved.signal();
		return x;
	}


	/**
	 * Enqueue a call to be batched, if it can be done immediately.
	 *
	 * @param request the request to be batched
	 * @return a future response if the call was batched, or {@code null} if the batch queue was full
	 */
	public CompletableFuture<Response> enqueue(final Request request) {
		requireNonNull(request);
		final CompletableFuture<Response> future;

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
	 * Enqueue a call to be batched, waiting up to the specified timeout for capacity to become available.
	 *
	 * @param request the request to be batched
	 * @param timeout the maximum time to wait for queue capacity
	 * @param unit    the unit of the parameter {@code timeout}
	 * @return a future response if the call was batched, or {@code null} if the batch queue was full
	 * @throws InterruptedException if this thread was interrupted while waiting
	 */
	public CompletableFuture<Response> enqueue(final Request request, final long timeout, final TimeUnit unit) throws InterruptedException {
		requireNonNull(request);
		long nanosToTimeout = unit.toNanos(timeout);
		final CompletableFuture<Response> future;

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
	 * Shuts down the batch queue. After this method has been called, no new elements can be enqueued and
	 * {@link #acquireBatch(long, TimeUnit, int, Collection)} will return {@code false} eventually (i.e. when the underlying queue is empty).
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
	}


	/**
	 * Check whether the batch queue has been shutdown. If so, calls to {@link #enqueue(Object)} and {@link #enqueue(Object, long, TimeUnit)} will always
	 * return {@code null}.
	 *
	 * @return {@code true} if the queue has been shutdown, {@code true} otherwise
	 */
	public boolean isShutdown() {
		final boolean result;
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
		final boolean result;
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
	 * @param timeout the maximum time to wait
	 * @param unit    the unit of the parameter {@code timeout}
	 * @return {@code true} if the queue was shhutdown and emptied within the timeout, {@code false} if not
	 * @throws InterruptedException if this thread was interrupted while waiting
	 */
	public boolean awaitShutdownComplete(final long timeout, final TimeUnit unit) throws InterruptedException {
		long nanosToTimeout = unit.toNanos(timeout);
		final boolean result;
		lock.lock();
		try {
			while (!isShutdownComplete()) {
				if (nanosToTimeout <= 0) {
					break;
				}
				// dequeue0 signals elementRemoved
				nanosToTimeout = elementRemoved.awaitNanos(nanosToTimeout);
			}
			result = isShutdownComplete();
		} finally {
			lock.unlock();
		}
		return result;
	}


	/**
	 * Acquire a batch of items from the queue. Waits up to the specified timeout for items to become available, and up to the specified linger time for a
	 * second item to become available. It returns after that, after having placed at most {@code maxElements} elements in the specified collection.
	 * <p>
	 * This means that this method lingers for at most {@code linger} {@code unit}s after it has acquired an item from the queue.
	 *
	 * @param timeout     the maximum amount of time to wait for a first item to become available
	 * @param unit        the unit of the parameters {@code timeout} and {@code linger}
	 * @param maxElements the maximum number of elements to put into {@code collection}
	 * @param collection  the collection to add the elements to
	 * @return {@code true} until both {@link #shutdown()} has been called and the underlying queue is empty, {@code false} after that
	 * @throws InterruptedException when the current thread was interrupted while waiting
	 */
	public boolean acquireBatch(final long timeout, final TimeUnit unit, final int maxElements, final Collection<BatchElement<Request, Response>> collection)
		throws InterruptedException {
		if (timeout < 0) {
			throw new IllegalArgumentException("timeout must be non-negative");
		}
		if (maxElements <= 0) {
			throw new IllegalArgumentException("maxElements must be positive");
		}
		requireNonNull(collection, "You must supply a collection");

		final boolean canYieldMoreBatches;

		lock.lockInterruptibly();
		try {
			int elementsInBatch = 0;
			long nanosToTimeout = unit.toNanos(timeout);
			while (elementsInBatch < maxElements && (count > 0 || !isShutdown && nanosToTimeout >= 0)) {
				while (!isShutdown && count == 0 && nanosToTimeout > 0) {
					nanosToTimeout = elementAdded.awaitNanos(nanosToTimeout);
				}
				if (count > 0) {
					// Using dequeue0() because it preserves invariants, just in case adding to the collection throws
					final BatchElement<Request, Response> element = dequeue0();
					if (elementsInBatch == 0) {
						// This is the first element in the batch, so now our timeout changes to the leftover linger time from the element
						nanosToTimeout = element.getDeadlineNanos() - System.nanoTime();
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
