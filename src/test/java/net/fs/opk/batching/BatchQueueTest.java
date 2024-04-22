package net.fs.opk.batching;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchQueueTest {
	private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	@Test
	@Timeout(value = 300, unit = MILLISECONDS)
	void verifyInvariants() throws InterruptedException {
		Predicate<BatchQueue<String, String>> testInvariants =
			it -> it.count <= it.items.length &&
			      it.count % it.items.length == (it.items.length + it.enqueueIndex - it.dequeueIndex) % it.items.length &&
			      it.count == Stream.of(it.items).filter(Objects::nonNull).count();
		Condition<BatchQueue<String, String>> validInvariants = new Condition<>(testInvariants, "has valid invariants");

		BatchQueue<String, String> queue = new BatchQueue<>(3, 10, NANOSECONDS, 10, MILLISECONDS);
		assertThat(queue).has(validInvariants).matches(q -> q.count == 0, "size");

		assertThat(queue.enqueue("one", 1, MILLISECONDS)).isNotNull();
		assertThat(queue).has(validInvariants).matches(q -> q.count == 1, "size");

		assertThat(queue.enqueue("two", 2, MILLISECONDS)).isNotNull();
		assertThat(queue).has(validInvariants).matches(q -> q.count == 2, "size");

		assertThat(queue.enqueue("three")).isNotNull();
		assertThat(queue).has(validInvariants).matches(q -> q.count == 3, "size");

		List<BatchElement<String, String>> batch = new ArrayList<>();
		assertThat(queue.acquireBatch(0, MILLISECONDS, 2, batch)).isTrue();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("one", "two");
		assertThat(queue).has(validInvariants).matches(q -> q.count == 1, "size");

		assertThat(queue.enqueue("four")).isNotNull();
		assertThat(queue).has(validInvariants).matches(q -> q.count == 2, "size");

		batch.clear();
		assertThat(queue.acquireBatch(0, MILLISECONDS, 2, batch)).isTrue();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("three", "four");
		assertThat(queue).has(validInvariants).matches(q -> q.count == 0, "size");

		queue.shutdown();
	}

	@Test
	@Timeout(value = 300, unit = MILLISECONDS)
	void verifyLimit() throws InterruptedException {
		BatchQueue<String, String> queue = new BatchQueue<>(3, 10, NANOSECONDS, 10, MILLISECONDS);
		assertThat(queue.enqueue("one", 2, MILLISECONDS)).isNotNull();
		assertThat(queue.enqueue("two", 2, MILLISECONDS)).isNotNull();
		assertThat(queue.enqueue("three", 2, MILLISECONDS)).isNotNull();
		assertThat(queue.enqueue("four", 2, MILLISECONDS)).isNull();
		assertThat(queue.enqueue("four")).isNull();

		queue.shutdown();
	}

	@Test
	@Timeout(value = 300, unit = MILLISECONDS)
	void verifyShutdown() throws InterruptedException {
		BatchQueue<String, String> queue = new BatchQueue<>(3, 10, NANOSECONDS, 10, MILLISECONDS);
		assertThat(queue.enqueue("one")).isNotNull();
		assertThat(queue.enqueue("two", 10, NANOSECONDS)).isNotNull();
		assertThat(queue.isShutdown()).isFalse();
		assertThat(queue.isShutdownComplete()).isFalse();
		assertThat(queue.awaitShutdownComplete(10, NANOSECONDS)).isFalse();

		queue.shutdown();

		assertThat(queue.enqueue("three")).isNull();
		assertThat(queue.enqueue("four", 10, NANOSECONDS)).isNull();
		assertThat(queue.isShutdown()).isTrue();
		assertThat(queue.isShutdownComplete()).isFalse();
		assertThat(queue.awaitShutdownComplete(10, NANOSECONDS)).isFalse();

		List<BatchElement<String, String>> batch = new ArrayList<>();
		assertThat(queue.acquireBatch(10, NANOSECONDS, 10, batch)).isFalse();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("one", "two");
		assertThat(queue.isShutdown()).isTrue();
		assertThat(queue.isShutdownComplete()).isTrue();
		assertThat(queue.awaitShutdownComplete(10, NANOSECONDS)).isTrue();
	}

	@Test
	void verifyInterruptingAwaitShutdownComplete() {
		BatchQueue<String, String> queue = new BatchQueue<>(3, 10, NANOSECONDS, 10, MILLISECONDS);
		Thread.currentThread().interrupt();
		assertThatThrownBy(() -> queue.awaitShutdownComplete(10, NANOSECONDS)).isInstanceOf(InterruptedException.class);

		queue.shutdown();
	}

	@Test
	void verifyArgumentChecksToAwaitShutdownComplete() {
		BatchQueue<String, String> queue = new BatchQueue<>(3, 10, NANOSECONDS, 10, MILLISECONDS);

		assertThatThrownBy(() -> queue.awaitShutdownComplete(-1, NANOSECONDS)).isInstanceOf(IllegalArgumentException.class);

		queue.shutdown();
	}

	@Test
	void verifyConstructionArgumentChecks() {
		// Capacity
		assertThatThrownBy(() -> new BatchQueue<>(0, 10, NANOSECONDS, 10, MILLISECONDS)).isInstanceOf(IllegalArgumentException.class);

		// Linger time
		assertThatThrownBy(() -> new BatchQueue<>(1, -1, NANOSECONDS, 10, MILLISECONDS)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> new BatchQueue<>(1, 110000, DAYS, 10, DAYS)).isInstanceOf(IllegalArgumentException.class); // Way too long...

		// Timeout
		assertThatThrownBy(() -> new BatchQueue<>(1, 10, NANOSECONDS, 10, NANOSECONDS)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> new BatchQueue<>(1, 10, NANOSECONDS, 110000, DAYS)).isInstanceOf(IllegalArgumentException.class); // Way too long...
	}

	@Test
	void verifyEnqueuingArgumentChecks() {
		BatchQueue<String, String> queue = new BatchQueue<>(3, 10, NANOSECONDS, 10, MILLISECONDS);

		assertThatThrownBy(() -> queue.enqueue(null)).isInstanceOf(NullPointerException.class);

		assertThatThrownBy(() -> queue.enqueue(null, 1, NANOSECONDS)).isInstanceOf(NullPointerException.class);
		assertThatThrownBy(() -> queue.enqueue("Foo", -1, NANOSECONDS)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> queue.enqueue("foo", 1, null)).isInstanceOf(NullPointerException.class);

		queue.shutdown();
	}

	@Test
	void verifyArgumentChecksToAqcuireBatch() {
		BatchQueue<String, String> queue = new BatchQueue<>(3, 10, NANOSECONDS, 10, MILLISECONDS);
		List<BatchElement<String, String>> list = new ArrayList<>();

		assertThatThrownBy(() -> queue.acquireBatch(-1, NANOSECONDS, 1, list)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> queue.acquireBatch(1, null, 1, list)).isInstanceOf(NullPointerException.class);
		assertThatThrownBy(() -> queue.acquireBatch(10, NANOSECONDS, 0, list)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> queue.acquireBatch(10, NANOSECONDS, 1, null)).isInstanceOf(NullPointerException.class);

		queue.shutdown();
	}

	@Test
	@Timeout(value = 300, unit = MILLISECONDS)
	void verifyAwaitingNewElements_Empty() throws InterruptedException {
		BatchQueue<String, String> queue = new BatchQueue<>(2, 10, NANOSECONDS, 10, MILLISECONDS);

		List<BatchElement<String, String>> batch = new ArrayList<>();
		assertThat(queue.acquireBatch(10, NANOSECONDS, 2, batch)).isTrue();
		assertThat(batch).isEmpty();

		queue.shutdown();

		assertThat(queue.acquireBatch(10, NANOSECONDS, 2, batch)).isFalse();
		assertThat(batch).isEmpty();
	}

	@Test
	@Timeout(value = 300, unit = MILLISECONDS)
	void verifyAwaitingNewElements_Single() throws InterruptedException {
		BatchQueue<String, String> queue = new BatchQueue<>(2, 10, NANOSECONDS, 10, MILLISECONDS);
		queue.enqueue("one");

		List<BatchElement<String, String>> batch = new ArrayList<>();
		assertThat(queue.acquireBatch(10, NANOSECONDS, 1, batch)).isTrue();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("one");

		queue.shutdown();
	}

	@Test
	@Timeout(value = 300, unit = MILLISECONDS)
	void verifyAwaitingNewElements_One() throws InterruptedException {
		BatchQueue<String, String> queue = new BatchQueue<>(2, 10, MILLISECONDS, 30, TimeUnit.SECONDS);
		queue.enqueue("one").cancel(false);
		queue.enqueue("two");
		queue.shutdown();

		List<BatchElement<String, String>> batch = new ArrayList<>();
		assertThat(queue.acquireBatch(10, NANOSECONDS, 10, batch)).isFalse();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("two");
	}

	@Test
	@Timeout(value = 300, unit = MILLISECONDS)
	void verifyAwaitingNewElements_ShortLinger() throws InterruptedException {
		BatchQueue<String, String> queue = new BatchQueue<>(2, 3, MILLISECONDS, 30, TimeUnit.SECONDS);
		List<BatchElement<String, String>> batch = new ArrayList<>();

		queue.enqueue("one");
		// Delay adding the 2nd element, but within the linger time of the 1st element
		delay(1, NANOSECONDS, () -> queue.enqueue("two"));
		assertThat(queue.acquireBatch(10, MILLISECONDS, 10, batch)).isTrue();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("one", "two");
		batch.clear();

		queue.enqueue("one");
		// Delay adding the 2nd element, but outsize the linger time of the 1st element
		delay(6, MILLISECONDS, () -> queue.enqueue("two"));
		// Acquire batch with timeout shorter than the linger time of the 1st element
		assertThat(queue.acquireBatch(10, MILLISECONDS, 10, batch)).isTrue();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("one");

		queue.shutdown();
	}

	private void delay(long delayMs, TimeUnit timeUnit, Runnable runnable) {
		executor.schedule(runnable, delayMs, timeUnit);
	}

	@Test
	@Timeout(value = 300, unit = MILLISECONDS)
	void verifyMetrics() throws InterruptedException {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		try {
			Metrics.globalRegistry.add(registry);

			BatchQueue<String, String> queue = new BatchQueue<>(2, 10, MILLISECONDS, 30, TimeUnit.SECONDS, "metrics_test");
			CompletableFuture<String> future = queue.enqueue("one");
			List<BatchElement<String, String>> batch = new ArrayList<>();
			assertThat(queue.acquireBatch(10, NANOSECONDS, 10, batch)).isTrue();
			Thread.sleep(50);
			future.complete("done");

			List<Meter> meters = Metrics.globalRegistry.getMeters();
			queue.shutdown();

			assertThat(meters).map(Meter::getId).map(Meter.Id::getName).filteredOn(s -> s.startsWith("metrics_test"))
				.containsExactlyInAnyOrder("metrics_test.queued", "metrics_test.processing");
			assertThat(meters).map(Meter::getId).filteredOn(id -> id.getName().startsWith("metrics_test")).map(Meter.Id::getType)
				.containsExactly(Meter.Type.TIMER, Meter.Type.TIMER);

			Map<String, Meter> meterMap = meters.stream().filter(m -> m.getId().getName().startsWith("metrics_test"))
				.collect(Collectors.toMap(m -> m.getId().getName(), m -> m));

			Timer queuedTimer = (Timer) meterMap.get("metrics_test.queued");
			assertThat(queuedTimer.count()).isEqualTo(1);
			double queuedTime = queuedTimer.totalTime(MILLISECONDS);

			Timer processingTimer = (Timer) meterMap.get("metrics_test.processing");
			assertThat(processingTimer.count()).isEqualTo(1);
			double processingTime = processingTimer.totalTime(MILLISECONDS);

			assertThat(queuedTime).isLessThan(10); // less than linger time: this 1st item was already there when acquiring the batch
			assertThat(processingTime - queuedTime).isLessThan(60); // 10ms linger + 50ms sleep, minus processing time
		} finally {
			Metrics.globalRegistry.remove(registry);
		}
	}
}
