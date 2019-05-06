package net.fs.opk.batching;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Stream;

import net.fs.opk.batching.BatchElement;
import net.fs.opk.batching.BatchQueue;
import org.assertj.core.api.Condition;
import org.junit.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class BatchQueueTest {

	@Test(timeout = 300)
	public void verifyInvariants() throws InterruptedException {
		final Predicate<BatchQueue> testInvariants =
				it -> it.count % it.items.length == (it.items.length + it.enqueueIndex - it.dequeueIndex) % it.items.length &&
				      it.count == Stream.of(it.items).filter(Objects::nonNull).count();
		final Condition<BatchQueue> validInvariants = new Condition<>(testInvariants, "has valid invariants");

		final BatchQueue<String, String> queue = new BatchQueue<>(3, 10, TimeUnit.NANOSECONDS);
		assertThat(queue).has(validInvariants).matches(q -> q.count == 0, "size");

		assertThat(queue.enqueue("one", 1, TimeUnit.MILLISECONDS)).isNotNull();
		assertThat(queue).has(validInvariants).matches(q -> q.count == 1, "size");

		assertThat(queue.enqueue("two", 2, TimeUnit.MILLISECONDS)).isNotNull();
		assertThat(queue).has(validInvariants).matches(q -> q.count == 2, "size");

		assertThat(queue.enqueue("three")).isNotNull();
		assertThat(queue).has(validInvariants).matches(q -> q.count == 3, "size");

		final List<BatchElement<String, String>> batch = new ArrayList<>();
		assertThat(queue.acquireBatch(0, TimeUnit.MILLISECONDS, 2, batch)).isTrue();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("one", "two");
		assertThat(queue).has(validInvariants).matches(q -> q.count == 1, "size");

		assertThat(queue.enqueue("four")).isNotNull();
		assertThat(queue).has(validInvariants).matches(q -> q.count == 2, "size");

		batch.clear();
		assertThat(queue.acquireBatch(0, TimeUnit.MILLISECONDS, 2, batch)).isTrue();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("three", "four");
		assertThat(queue).has(validInvariants).matches(q -> q.count == 0, "size");
	}

	@Test(timeout = 300)
	public void verifyLimit() throws InterruptedException {
		final BatchQueue<String, String> queue = new BatchQueue<>(3, 10, TimeUnit.NANOSECONDS);
		assertThat(queue.enqueue("one", 2, TimeUnit.MILLISECONDS)).isNotNull();
		assertThat(queue.enqueue("two", 2, TimeUnit.MILLISECONDS)).isNotNull();
		assertThat(queue.enqueue("three", 2, TimeUnit.MILLISECONDS)).isNotNull();
		assertThat(queue.enqueue("four", 2, TimeUnit.MILLISECONDS)).isNull();
		assertThat(queue.enqueue("four")).isNull();
	}


	@Test(timeout = 300)
	public void verifyShutdown() throws InterruptedException {
		final BatchQueue<String, String> queue = new BatchQueue<>(3, 10, TimeUnit.NANOSECONDS);
		assertThat(queue.enqueue("one")).isNotNull();
		assertThat(queue.enqueue("two", 10, TimeUnit.NANOSECONDS)).isNotNull();
		assertThat(queue.isShutdown()).isFalse();
		assertThat(queue.isShutdownComplete()).isFalse();
		assertThat(queue.awaitShutdownComplete(10, TimeUnit.NANOSECONDS)).isFalse();

		queue.shutdown();

		assertThat(queue.enqueue("three")).isNull();
		assertThat(queue.enqueue("four", 10, TimeUnit.NANOSECONDS)).isNull();
		assertThat(queue.isShutdown()).isTrue();
		assertThat(queue.isShutdownComplete()).isFalse();
		assertThat(queue.awaitShutdownComplete(10, TimeUnit.NANOSECONDS)).isFalse();

		final List<BatchElement<String, String>> batch = new ArrayList<>();
		assertThat(queue.acquireBatch(10, TimeUnit.NANOSECONDS, 10, batch)).isFalse();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("one", "two");
		assertThat(queue.isShutdown()).isTrue();
		assertThat(queue.isShutdownComplete()).isTrue();
		assertThat(queue.awaitShutdownComplete(10, TimeUnit.NANOSECONDS)).isTrue();
	}


	@Test
	public void verifyArgumentChecks() {
		assertThatThrownBy(() -> new BatchQueue<>(0, 10, TimeUnit.NANOSECONDS)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> new BatchQueue<>(1, -1, TimeUnit.NANOSECONDS)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> new BatchQueue<>(1, 110000, TimeUnit.DAYS)).isInstanceOf(IllegalArgumentException.class); // Way too long...

		final BatchQueue<String, String> queue = new BatchQueue<>(3, 10, TimeUnit.NANOSECONDS);
		final List<BatchElement<String, String>> list = new ArrayList<>();
		assertThatThrownBy(() -> queue.acquireBatch(-1, TimeUnit.NANOSECONDS, 1, list)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> queue.acquireBatch(10, TimeUnit.NANOSECONDS, 0, list)).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> queue.acquireBatch(10, TimeUnit.NANOSECONDS, 1, null)).isInstanceOf(NullPointerException.class);
	}


	@Test(timeout = 300)
	public void verifyAwaitingNewElements_Empty() throws InterruptedException {
		final BatchQueue<String, String> queue = new BatchQueue<>(2, 10, TimeUnit.NANOSECONDS);

		final List<BatchElement<String, String>> batch = new ArrayList<>();
		assertThat(queue.acquireBatch(10, TimeUnit.NANOSECONDS, 2, batch)).isTrue();
		assertThat(batch).isEmpty();

		queue.shutdown();

		assertThat(queue.acquireBatch(10, TimeUnit.NANOSECONDS, 2, batch)).isFalse();
		assertThat(batch).isEmpty();
	}


	@Test(timeout = 300)
	public void verifyAwaitingNewElements_Single() throws InterruptedException {
		final BatchQueue<String, String> queue = new BatchQueue<>(2, 10, TimeUnit.NANOSECONDS);
		queue.enqueue("one");

		final List<BatchElement<String, String>> batch = new ArrayList<>();
		assertThat(queue.acquireBatch(10, TimeUnit.NANOSECONDS, 1, batch)).isTrue();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("one");
	}


	@Test(timeout = 300)
	public void verifyAwaitingNewElements_One() throws InterruptedException {
		final BatchQueue<String, String> queue = new BatchQueue<>(2, 10, TimeUnit.NANOSECONDS);
		queue.enqueue("one");
		queue.shutdown();

		final List<BatchElement<String, String>> batch = new ArrayList<>();
		assertThat(queue.acquireBatch(10, TimeUnit.NANOSECONDS, 2, batch)).isFalse();
		assertThat(batch).extracting(BatchElement::getInputValue).containsExactly("one");
	}
}
