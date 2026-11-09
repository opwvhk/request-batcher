package net.fs.opk.batching;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import net.fs.opk.util.Namer;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * A capacity limited queue to submit calls to that will be executed in batches. Its main use is for a
 * {@link BatchRunner} running in a separate thread. As this is a multithreaded process, this queue can be shutdown
 * after which no new elements can be added. Shutdown is complete when the queue has been shutdown and emptied.
 *
 * <p>This queue does not implement {@link BlockingQueue} nor {@link Queue}, because it is not intended to be used as a
 * {@link Collection}, but rather as a grouping mechanism. This is also why {@link #enqueue(Object)} and
 * {@link #enqueue(Object, long, TimeUnit)} return a {@link CompletableFuture}.</p>
 *
 * <h2>Throughput, latency and order</h2>
 *
 * <p>In addition to method specific timeouts, elements on the queue also have a 'timeout': the linger time (specified
 * when constructing the queue). The linger time specifies the amount of time an item on the queue is willing to wait
 * for a batch to fill. When it expires, the item will be picked up in a batch as soon as possible. Larger linger times
 * directly affect latency (it's the minimum latency for the first item in a batch) and increase the probability to
 * batch multiple elements together, thus indirectly increasing throughput.</p>
 *
 * <p>The order of items on the queue is determined by the order the elements are added. This order is kept when
 * consuming batches off the queue using a single thread, but not when consuming the queue with multiple threads: if the
 * queue contains elements [A, B, C, D, E], two threads simultaneously acquiring a batch may receive the batches [A, C,
 * D] and [B, E] respectively.</p>
 */
public class BatchQueue<Request, Response> {
	public static final String OBSERVATION_NAME_REQUEST_BATCH = "request_batch";
	public static final String OBSERVATION_NAME_QUEUE = "queue";
	public static final String OBSERVATION_NAME_PROCESSING = "processing";
	private static final Namer OBSERVATION_NAMER = new Namer("batch_queue_");
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
	 */
	private int count;
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
	 * Condition to await free capacity. Both decrementing {@code count} and setting {@code isShutdown} to {@code true}
	 * signals this condition.
	 */
	private final Condition elementRemoved;
	/**
	 * Condition to await new elements. Both incrementing {@code count} and setting {@code isShutdown} to {@code true}
	 * signals this condition.
	 */
	private final Condition elementAdded;

	/*
	 * Other fields.
	 */

	/**
	 * The number of nanoseconds an element may linger to await more elements. Elements will be on the queue at least
	 * this long, except when the batch they end up in is full sooner.
	 */
	private final long lingerNs;
	/**
	 * The timeout (in ns) for enqueued elements to complete. This is the timeout on the queue plus all processing
	 * until the final result.
	 */
	private final long timeoutNs;
	/**
	 * The name of the queue. Can be used by
	 * {@link io.micrometer.observation.ObservationConvention ObservationConvention&lt;BatchQueueContext&lt;Request, Response&gt;&gt;}
	 * implementations to add queue-specific tags to the observations.
	 */
	private final String queueName;
	/**
	 * The observation registry to use.
	 */
	private final ObservationRegistry registry;

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
		this(capacity, linger, lingerUnit, timeout, timeoutUnit, null, null);
	}

	/**
	 * Create a batch queue with the given capacity.
	 *
	 * @param capacity    the maximum number of items on the queue
	 * @param linger      the time an item may wait on the queue to enlarge the batch
	 * @param lingerUnit  the unit of the parameter {@code linger}
	 * @param timeout     the maximum time until enqueued items must have a result
	 * @param timeoutUnit the unit of the parameter {@code timeout}
	 * @param queueName   the name of the queue, used for metrics and tracing
	 */
	public BatchQueue(int capacity, long linger, TimeUnit lingerUnit, long timeout, TimeUnit timeoutUnit,
					  @Nullable String queueName, @Nullable ObservationRegistry registry) {
		if (capacity <= 0) {
			throw new IllegalArgumentException("Capacity must be positive");
		}
		lingerNs = lingerUnit.toNanos(linger);
		if (lingerNs < 0 || lingerNs == Long.MAX_VALUE) {
			throw new IllegalArgumentException(
				"The linger time must be non-negative and less than 2^63-1 ns (approx. 292 years)");
		}
		timeoutNs = timeoutUnit.toNanos(timeout);
		if (timeoutNs <= lingerNs || timeoutNs == Long.MAX_VALUE) {
			throw new IllegalArgumentException(
				"The timeout must be larger than the linger time and less than 2^63-1 ns (approx. 292 years)");
		}

		items = (BatchElement<Request, Response>[]) new BatchElement[capacity];
		enqueueIndex = 0;
		dequeueIndex = 0;
		count = 0;

		lock = new ReentrantLock();
		elementRemoved = lock.newCondition();
		elementAdded = lock.newCondition();

		this.queueName = OBSERVATION_NAMER.name(queueName);
		if (registry != null) {
			this.registry = registry;
		} else if (!Metrics.globalRegistry.getRegistries().isEmpty()) {
			DefaultMeterObservationHandler metricsHandler = new DefaultMeterObservationHandler(Metrics.globalRegistry);
			this.registry = ObservationRegistry.create();
			this.registry.observationConfig().observationHandler(metricsHandler);
		} else {
			this.registry = ObservationRegistry.NOOP;
		}
	}

	/**
	 * Enqueue an item onto this batch queue. Callers must acquire the lock, and ensure that the queue is not full.
	 *
	 * @param item the item to enqueue
	 * @return the result future for the enqueued item
	 */
	private CompletableFuture<Response> enqueue0(Request item) {
		// Assumes we hold the lock and count < items.length (and thus items[enqueueIndex] == null)

		long lingerDeadline = System.nanoTime() + lingerNs;

		Supplier<BatchQueueContext<Request, Response>> contextSupplier = () -> new BatchQueueContext<>(this, item);
		Observation observation = Observation.start(OBSERVATION_NAME_REQUEST_BATCH, contextSupplier, registry);
		Observation queueObservation = Observation.createNotStarted(OBSERVATION_NAME_QUEUE, contextSupplier, registry)
			.parentObservation(observation)
			.start();
		Observation processingObservation = Observation
			.createNotStarted(OBSERVATION_NAME_PROCESSING, contextSupplier, registry)
			.parentObservation(observation);
		BatchElement<Request, Response> element = new BatchElement<>(item, lingerDeadline, timeoutNs);
		element.queueFuture.whenComplete((ignored1, error) -> {
			if (error != null) {
				// Dequeued only; result was already known
				queueObservation.error(error);
			}
			queueObservation.stop();
			if (error == null) {
				// Dequeued for processing
				processingObservation.start();
				element.outputFuture.whenComplete((ignored2, processingError) -> {
					if (processingError != null) {
						processingObservation.error(processingError);
					}
					processingObservation.stop();
				});
			}
		});
		element.outputFuture.whenComplete(
			(ignored, error) -> {
				if (error != null) {
					observation.error(error);
				}
				observation.stop();
			});
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
	 * Dequeue the head of the queue. Callers must acquire the lock, ensure that the queue is not empty, and use
	 * {@link #peekAtQueue()} to get the actual element.
	 */
	private void dequeue0(BatchElement<Request, Response> element) {
		// Assumes we hold the lock and count > 0 (and thus items[dequeueIndex] != null)

		assert items[dequeueIndex] == element;
		items[dequeueIndex] = null;

		dequeueIndex++;
		if (dequeueIndex == items.length) {
			dequeueIndex = 0;
		}
		count--;
		elementRemoved.signal();
	}

	/**
	 * Peek at the head of this batch queue. Callers must acquire the lock, and ensure that the queue is not empty.
	 *
	 * @return the head of the queue (it is still on the queue!)
	 */
	private BatchElement<Request, Response> peekAtQueue() {
		// Assumes we hold the lock and count > 0 (and thus items[dequeueIndex] != null)

		return items[dequeueIndex];
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
	 * @return a future response if successful, or {@code null} if the timeout elapsed before there was a place on the
	 * batch queue
	 * @throws InterruptedException if this thread was interrupted while waiting
	 */
	public CompletableFuture<Response> enqueue(Request request, long timeout, TimeUnit unit)
		throws InterruptedException {
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
	 * Return the size of the queue.
	 *
	 * @return the number of elements in the queue
	 */
	public int size() {
		lock.lock();
		try {
			return count;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Return the name of the queue.
	 *
	 * @return the name of the queue
	 */
	public String name() {
		return queueName;
	}

	/**
	 * Shutdown the batch queue. After this method has been called, no new elements can be added and
	 * {@link #acquireBatch(long, TimeUnit, int, Collection)} will return {@code false} eventually (i.e., when the
	 * underlying queue is empty).
	 */
	public void shutdown() {
		lock.lock();
		try {
			isShutdown = true;
			// Signal all waiting threads: as the queue can only be emptied now, there's no need to wait for capacity
			// nor wait for new elements.
			elementRemoved.signal();
			elementAdded.signal();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Check whether the batch queue has been shutdown. If so, calls to {@link #enqueue(Object)} and
	 * {@link #enqueue(Object, long, TimeUnit)} will always return {@code null}.
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
	 * Check whether the batch queue has been shutdown and emptied. If so, calls to {@link #enqueue(Object)} and
	 * {@link #enqueue(Object, long, TimeUnit)} will always return {@code null}, and calls to
	 * {@link #acquireBatch(long, TimeUnit, int, Collection)} will always return {@code false}.
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
	 * <p>Acquire a batch of items from the queue. Waits up to the specified timeout for items to become available, and
	 * up to the specified linger time for a second item to become available. It returns after that, after having
	 * placed at most {@code maxElements} elements in the specified collection.</p>
	 *
	 * <p>This method lingers for at most {@code linger} {@code unit}s after it has acquired an item from the queue.
	 * So for a non-empty queue, the minimum of the linger time and timeout is the actual timeout.</p>
	 *
	 * @param timeout     the maximum amount of time to wait for a first item to become available; must be non-negative
	 * @param unit        the unit of the {@code timeout} parameter
	 * @param maxElements the maximum number of elements to put into {@code collection}
	 * @param collection  the collection to add the elements to
	 * @return {@code true} until both {@link #shutdown()} has been called and the underlying queue is empty,
	 * {@code false} after that
	 * @throws InterruptedException when the current thread was interrupted while waiting
	 */
	public boolean acquireBatch(long timeout, TimeUnit unit, int maxElements,
								Collection<BatchElement<Request, Response>> collection)
		throws InterruptedException {
		if (maxElements <= 0) {
			throw new IllegalArgumentException("maxElements must be positive");
		}
		AtomicInteger count = new AtomicInteger();
		return acquireBatch(timeout, unit, ignored -> count.getAndIncrement() < maxElements, collection);
	}

	/**
	 * <p>Acquire a batch of items from the queue. Waits up to the specified timeout for items to become available, and
	 * up to the specified linger time for a second item to become available. Each element is checked if it can be
	 * acquired using the specified predicate, and acquisition stops at the first failure. The predicate may be
	 * stateful; in fact, the method {@link #acquireBatch(long, TimeUnit, int, Collection)} uses a stateful
	 * predicate to
	 * count the number of elements in the batch.</p>
	 *
	 * <p>This method lingers for at most {@code linger} {@code unit}s after it has acquired an item from the queue.
	 * So for a non-empty queue, the minimum of the linger time and timeout is the actual timeout.</p>
	 *
	 * @param timeout       the maximum amount of time to wait for a first item to become available; must be
	 *                      non-negative
	 * @param unit          the unit of the {@code timeout} parameter
	 * @param acceptRequest predicate to decide whether to accept the next request
	 * @param collection    the collection to add the elements to
	 * @return {@code true} until both {@link #shutdown()} has been called and the underlying queue is empty,
	 * {@code false} after that
	 * @throws InterruptedException when the current thread was interrupted while waiting
	 */
	public boolean acquireBatch(long timeout, TimeUnit unit, Predicate<Request> acceptRequest,
								Collection<BatchElement<Request, Response>> collection)
		throws InterruptedException {
		if (timeout < 0) {
			throw new IllegalArgumentException("timeout must be non-negative");
		}
		requireNonNull(acceptRequest, "You must supply a request acceptance predicate");
		requireNonNull(collection, "You must supply a collection");

		boolean canYieldMoreBatches;

		lock.lockInterruptibly();
		try {
			long nanosToTimeout = unit.toNanos(timeout);
			while (count > 0 || !isShutdown && nanosToTimeout >= 0) {
				while (!isShutdown && count == 0 && nanosToTimeout > 0) {
					nanosToTimeout = elementAdded.awaitNanos(nanosToTimeout);
				}
				if (count > 0) {
					// Using peekAtQueue() and dequeue0() to preserve invariants
					BatchElement<Request, Response> element = peekAtQueue();
					if (element.outputFuture.isDone()) {
						// Already done (probably cancelled); remove it from the queue, but don't add to the batch.
						dequeue0(element);
						// Complete the queue future with the error (or a new one if needed) to report queuing ended without starting processing
						element.outputFuture.whenComplete((ignored, error) -> element.queueFuture.completeExceptionally(
							Objects.requireNonNullElseGet(error, AlreadyCompletedException::new)));
						continue;
					}
					if (!acceptRequest.test(element.getInputValue())) {
						// The element was rejected: our batch is complete.
						break;
					}
					// The element was accepted: add it to our batch.
					dequeue0(element);
					element.queueFuture.complete(null);

					long lingerTimeLeft =
						element.lingerDeadlineNanos - System.nanoTime(); // Can become negative (by design)
					// Update our timeout based on the remaining linger time
					// (which is monotonically increasing, so only the first linger time has any effect)
					nanosToTimeout = Math.min(nanosToTimeout, lingerTimeLeft);

					collection.add(element);
				}
			}

			canYieldMoreBatches = !isShutdown || count > 0;
		} finally {
			lock.unlock();
		}
		return canYieldMoreBatches;
	}

	private static class AlreadyCompletedException extends RuntimeException {
		public AlreadyCompletedException() {
			super("The output future has already completed (successfully)", null, false, false);
		}
	}
}
