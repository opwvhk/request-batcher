package net.fs.opk.batching;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchQueueTest {

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
	}

	@Test
	void verifyArgumentChecksToAqcuireBatch() {
		BatchQueue<String, String> queue = new BatchQueue<>(3, 10, NANOSECONDS, 10, MILLISECONDS);
		List<BatchElement<String, String>> list = new ArrayList<>();

		assertThatThrownBy(() -> queue.acquireBatch(-1, NANOSECONDS, 1, list)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> queue.acquireBatch(1, null, 1, list)).isInstanceOf(NullPointerException.class);
		assertThatThrownBy(() -> queue.acquireBatch(10, NANOSECONDS, 0, list)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> queue.acquireBatch(10, NANOSECONDS, 1, null)).isInstanceOf(NullPointerException.class);
	}

	@Test
	void verifyArgumentChecksToAwaitShutdownComplete() {
		BatchQueue<String, String> queue = new BatchQueue<>(3, 10, NANOSECONDS, 10, MILLISECONDS);

		assertThatThrownBy(() -> queue.awaitShutdownComplete(-1, NANOSECONDS)).isInstanceOf(IllegalArgumentException.class);
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
	}

	@Test
	@Timeout(value = 300, unit = MILLISECONDS)
	void verifyAwaitingNewElements_One() throws InterruptedException {
		BatchQueue<String, String> queue = new BatchQueue<>(2, 10, MILLISECONDS, 30, TimeUnit.SECONDS);
		queue.enqueue("one");
		queue.shutdown();

		List<BatchElement<String, String>> batch = new ArrayList<>();
		assertThat(queue.acquireBatch(10, NANOSECONDS, 2, batch)).isFalse();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("one");
	}
}
