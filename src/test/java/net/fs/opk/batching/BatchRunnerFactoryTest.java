package net.fs.opk.batching;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyCollection;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BatchRunnerFactoryTest {
	private static final int BATCH_SIZE = 3;

	private BatchQueue<String, String> queue;
	private BatchElement<String, String> e1, e2, e3, e4, e5;
	private Consumer<List<BatchElement<String, String>>> batchHandler;

	private BatchRunner<String, String> runner;

	@BeforeEach
	void setUp() throws InterruptedException {
		e1 = new BatchElement<>("one", 42L, 42_000_000_000L);
		e2 = new BatchElement<>("two", 42L, 42_000_000_000L);
		e3 = new BatchElement<>("three", 42L, 42_000_000_000L);
		e4 = new BatchElement<>("four", 42L, 42_000_000_000L);
		e5 = new BatchElement<>("five", 42L, 42_000_000_000L);
		AtomicInteger counter = new AtomicInteger(2);

		queue = (BatchQueue<String, String>) mock(BatchQueue.class);
		when(queue.acquireBatch(anyLong(), any(TimeUnit.class), anyInt(), anyCollection())).thenAnswer(invocation -> {
			assertThat((TimeUnit) invocation.getArgument(1)).isEqualTo(SECONDS);
			assertThat((Integer) invocation.getArgument(2)).isEqualTo(BATCH_SIZE);
			Collection<BatchElement<String, String>> batch = (Collection<BatchElement<String, String>>) invocation.getArgument(3, Collection.class);
			switch (counter.getAndDecrement()) {
				case 2 -> {
					batch.addAll(asList(e1, e2, e3));
					return true;
				}
				case 1 -> {
					return true;
				}
				default -> {
					batch.addAll(asList(e4, e5));
					return false;
				}
			}
		});

		batchHandler = (Consumer<List<BatchElement<String, String>>>) mock(Consumer.class);

		runner = BatchRunnerFactory.forConsumer(queue, BATCH_SIZE, batchHandler);
	}

	@Test
	@Timeout(value = 50, unit = MILLISECONDS)
	void validateHappyFlow() throws InterruptedException {
		doAnswer(invocation -> {
			List<BatchElement<String, String>> batch = (List<BatchElement<String, String>>) invocation.getArgument(0, List.class);
			batch.forEach(element -> {
				element.success("[" + element.getInputValue() + "]");
				System.out.printf("%s -> %s\n", element.getInputValue(), element.outputFuture.getNow("-"));
			});
			return null;
		}).when(batchHandler).accept(anyList());

		runner.run();

		verify(queue, times(3)).acquireBatch(anyLong(), eq(SECONDS), anyInt(), anyCollection());

		assertThat(e1.outputFuture).isCompletedWithValue("[one]");
		assertThat(e2.outputFuture).isCompletedWithValue("[two]");
		assertThat(e3.outputFuture).isCompletedWithValue("[three]");
		assertThat(e4.outputFuture).isCompletedWithValue("[four]");
		assertThat(e5.outputFuture).isCompletedWithValue("[five]");
	}

	@Test
	@Timeout(value = 300, unit = MILLISECONDS)
	void validateBatchStartCrashing() throws InterruptedException {
		RuntimeException error = new RuntimeException("test error");
		doThrow(error).when(batchHandler).accept(anyList());

		runner.run();

		verify(queue, times(3)).acquireBatch(anyLong(), eq(SECONDS), anyInt(), anyCollection());

		assertThat(e1.outputFuture).failsWithin(Duration.ZERO).withThrowableOfType(Exception.class).withCause(error);
		assertThat(e2.outputFuture).failsWithin(Duration.ZERO).withThrowableOfType(Exception.class).withCause(error);
		assertThat(e3.outputFuture).failsWithin(Duration.ZERO).withThrowableOfType(Exception.class).withCause(error);
		assertThat(e4.outputFuture).failsWithin(Duration.ZERO).withThrowableOfType(Exception.class).withCause(error);
		assertThat(e5.outputFuture).failsWithin(Duration.ZERO).withThrowableOfType(Exception.class).withCause(error);
	}

	@Test
	@Timeout(value = 50, unit = MILLISECONDS)
	void validateTermination() throws InterruptedException {
		when(queue.acquireBatch(anyLong(), any(TimeUnit.class), anyInt(), anyCollection())).thenThrow(new InterruptedException());

		runner.run();

		assertThat(e1.outputFuture).isNotDone();
		assertThat(e2.outputFuture).isNotDone();
		assertThat(e3.outputFuture).isNotDone();
		assertThat(e4.outputFuture).isNotDone();
		assertThat(e5.outputFuture).isNotDone();
	}

	@Test
	@Timeout(value = 50, unit = MILLISECONDS)
	void validateAbnormalTermination() throws InterruptedException {
		RuntimeException exception = new RuntimeException();
		when(queue.acquireBatch(anyLong(), any(TimeUnit.class), anyInt(), anyCollection())).thenThrow(exception);

		runner.run();

		assertThat(e1.outputFuture).isNotDone();
		assertThat(e2.outputFuture).isNotDone();
		assertThat(e3.outputFuture).isNotDone();
		assertThat(e4.outputFuture).isNotDone();
		assertThat(e5.outputFuture).isNotDone();
	}

	@Test
	@Timeout(value = 20_100, unit = MILLISECONDS)
	void verifyMaximumConcurrentBatches() {
		final int capacity = 1_000;
		BatchQueue<Integer, Integer> largeQueue = new BatchQueue<>(capacity, 1, SECONDS, 100, SECONDS);
		for (int i = 0; i < capacity; i++) {
			largeQueue.enqueue(i);
		}
		largeQueue.shutdown();

		AtomicInteger counter = new AtomicInteger(0);
		AtomicInteger errors = new AtomicInteger(0);
		final int maxConcurrentBatches = 5;
		ScheduledExecutorService pool = Executors.newScheduledThreadPool(2 * maxConcurrentBatches);
		BatchRunnerFactory.forConsumer(largeQueue, 2, maxConcurrentBatches, 1, SECONDS, batch -> {
			counter.incrementAndGet();
			pool.schedule(() -> {
				int concurrent = counter.getAndDecrement();
				if (concurrent > maxConcurrentBatches) {
					errors.incrementAndGet();
				}
				batch.forEach(e -> e.success(concurrent));
			}, 10, TimeUnit.MILLISECONDS);
		}).run();

		assertThat(errors).hasValue(0);
	}

	@Test
	@Timeout(value = 20_100, unit = MILLISECONDS)
	void verifyEmptyBatch() {
		BatchQueue<Integer, Integer> queue = new BatchQueue<>(1, 1, SECONDS, 100, SECONDS);
		queue.shutdown();

		BatchRunnerFactory.forConsumer(queue, 2, 5, 1, SECONDS, batch -> fail("There is no batch.")).run();
	}

	@Test
	void verifyMultiplexingWithFullResultsWorks() {
		Consumer<List<BatchElement<String, String>>> batchConsumer = BatchRunnerFactory.multiplexOverFunction(Function.identity(),
			CompletableFuture::completedFuture, (batch, results) -> {
				for (int i = 0; i < batch.size(); i++) {
					results.get(i).success("[" + batch.get(i) + "]");
				}
			});
		batchConsumer.accept(asList(e1, e2, e3, e4, e5));

		assertThat(e1.outputFuture).isCompletedWithValue("[one]");
		assertThat(e2.outputFuture).isCompletedWithValue("[two]");
		assertThat(e3.outputFuture).isCompletedWithValue("[three]");
		assertThat(e4.outputFuture).isCompletedWithValue("[four]");
		assertThat(e5.outputFuture).isCompletedWithValue("[five]");
	}

	@Test
	void verifyMultiplexingWithPartialResultsWorks() {
		Consumer<List<BatchElement<String, String>>> batchConsumer = BatchRunnerFactory.multiplexOverFunction(Function.identity(),
			CompletableFuture::completedFuture, (batch, results) -> {
				for (int i = 0; i < batch.size(); i++) {
					if (i == 2) {
						continue;
					}
					results.get(i).success("[" + batch.get(i) + "]");
				}
			});
		batchConsumer.accept(asList(e1, e2, e3, e4, e5));

		assertThat(e1.outputFuture).isCompletedWithValue("[one]");
		assertThat(e2.outputFuture).isCompletedWithValue("[two]");
		assertThat(e3.outputFuture).isCompletedExceptionally().isNotCancelled();
		assertThat(e4.outputFuture).isCompletedWithValue("[four]");
		assertThat(e5.outputFuture).isCompletedWithValue("[five]");
	}

	@Test
	void verifyMultiplexingWithFailureWorks() {
		RuntimeException exception = new RuntimeException();
		Consumer<List<BatchElement<String, String>>> batchConsumer = BatchRunnerFactory.multiplexOverFunction(Function.identity(),
			CompletableFuture::completedFuture, (batch, results) -> {
				throw exception;
			});
		batchConsumer.accept(asList(e1, e2, e3, e4, e5));

		assertThat(e1.outputFuture).failsWithin(Duration.ZERO).withThrowableOfType(Exception.class).withCause(exception);
		assertThat(e2.outputFuture).failsWithin(Duration.ZERO).withThrowableOfType(Exception.class).withCause(exception);
		assertThat(e3.outputFuture).failsWithin(Duration.ZERO).withThrowableOfType(Exception.class).withCause(exception);
		assertThat(e4.outputFuture).failsWithin(Duration.ZERO).withThrowableOfType(Exception.class).withCause(exception);
		assertThat(e5.outputFuture).failsWithin(Duration.ZERO).withThrowableOfType(Exception.class).withCause(exception);
	}
}
