package net.fs.opk.batching;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.*;


public class BatchRunnerFactoryTest {
	private static final int BATCH_SIZE = 3;

	private BatchQueue<String, String> queue;
	private BatchElement<String, String> e1, e2, e3, e4, e5;
	private Consumer<List<BatchElement<String, String>>> batchHandler;

	private BatchRunner<String, String> runner;


	@Before
	public void setUp() throws InterruptedException {
		e1 = new BatchElement<>(42L, "one");
		e2 = new BatchElement<>(42L, "two");
		e3 = new BatchElement<>(42L, "three");
		e4 = new BatchElement<>(42L, "four");
		e5 = new BatchElement<>(42L, "five");
		final AtomicInteger counter = new AtomicInteger(2);

		queue = (BatchQueue<String, String>)mock(BatchQueue.class);
		when(queue.acquireBatch(anyLong(), any(TimeUnit.class), anyInt(), anyCollection())).thenAnswer(invocation -> {
			assertThat((TimeUnit)invocation.getArgument(1)).isEqualTo(TimeUnit.SECONDS);
			assertThat((Integer)invocation.getArgument(2)).isEqualTo(BATCH_SIZE);
			final Collection<BatchElement<String, String>> batch = (Collection<BatchElement<String, String>>)invocation.getArgument(3, Collection.class);
			switch (counter.getAndDecrement()) {
				case 2:
					batch.addAll(asList(e1, e2, e3));
					return true;
				case 1:
					return true;
				default:
					batch.addAll(asList(e4, e5));
					return false;
			}
		});

		batchHandler = (Consumer<List<BatchElement<String, String>>>)mock(Consumer.class);

		runner = BatchRunnerFactory.forConsumer(queue, BATCH_SIZE, batchHandler);
	}


	@Test(timeout = 50)
	public void validateHappyFlow() throws InterruptedException {
		doAnswer(invocation -> {
			final List<BatchElement<String, String>> batch = (List<BatchElement<String, String>>)invocation.getArgument(0, List.class);
			batch.forEach(element -> {
				element.success("[" + element.getInputValue() + "]");
				System.out.printf("%s -> %s\n", element.getInputValue(), element.outputFuture.getNow("-"));
			});
			return null;
		}).when(batchHandler).accept(anyList());

		runner.run();

		verify(queue, times(3)).acquireBatch(anyLong(), eq(TimeUnit.SECONDS), anyInt(), anyCollection());

		assertThat(e1.outputFuture).isCompletedWithValue("[one]");
		assertThat(e2.outputFuture).isCompletedWithValue("[two]");
		assertThat(e3.outputFuture).isCompletedWithValue("[three]");
		assertThat(e4.outputFuture).isCompletedWithValue("[four]");
		assertThat(e5.outputFuture).isCompletedWithValue("[five]");
	}


	@Test(timeout = 300)
	public void validateBatchStartCrashing() throws InterruptedException {
		final Exception error = new RuntimeException("test error");
		doThrow(error).when(batchHandler).accept(anyList());

		runner.run();

		verify(queue, times(3)).acquireBatch(anyLong(), eq(TimeUnit.SECONDS), anyInt(), anyCollection());

		assertThat(e1.outputFuture).hasFailedWithThrowableThat().isSameAs(error);
		assertThat(e2.outputFuture).hasFailedWithThrowableThat().isSameAs(error);
		assertThat(e3.outputFuture).hasFailedWithThrowableThat().isSameAs(error);
		assertThat(e4.outputFuture).hasFailedWithThrowableThat().isSameAs(error);
		assertThat(e5.outputFuture).hasFailedWithThrowableThat().isSameAs(error);
	}


	@Test(timeout = 50)
	public void validateTermination() throws InterruptedException {
		when(queue.acquireBatch(anyLong(), any(TimeUnit.class), anyInt(), anyCollection())).thenThrow(new InterruptedException());

		runner.run();

		assertThat(e1.outputFuture).isNotDone();
		assertThat(e2.outputFuture).isNotDone();
		assertThat(e3.outputFuture).isNotDone();
		assertThat(e4.outputFuture).isNotDone();
		assertThat(e5.outputFuture).isNotDone();
	}


	@Test(timeout = 50)
	public void validateAbnormalTermination() throws InterruptedException {
		final RuntimeException exception = new RuntimeException();
		when(queue.acquireBatch(anyLong(), any(TimeUnit.class), anyInt(), anyCollection())).thenThrow(exception);

		runner.run();

		assertThat(e1.outputFuture).isNotDone();
		assertThat(e2.outputFuture).isNotDone();
		assertThat(e3.outputFuture).isNotDone();
		assertThat(e4.outputFuture).isNotDone();
		assertThat(e5.outputFuture).isNotDone();
	}


	@Test(timeout = 20_100)
	public void verifyMaximumConcurrentBatches() {
		final int capacity = 1_000;
		final BatchQueue<Integer, Integer> largeQueue = new BatchQueue<>(capacity, 1, TimeUnit.SECONDS);
		for (int i = 0; i < capacity; i++) {
			largeQueue.enqueue(i);
		}
		largeQueue.shutdown();

		final AtomicInteger counter = new AtomicInteger(0);
		final AtomicInteger errors = new AtomicInteger(0);
		final int maxConcurrentBatches = 5;
		final ScheduledExecutorService pool = Executors.newScheduledThreadPool(2 * maxConcurrentBatches);
		BatchRunnerFactory.forConsumer(largeQueue, 2, maxConcurrentBatches, 1, TimeUnit.SECONDS, batch -> {
			counter.incrementAndGet();
			pool.schedule(() -> {
				final int concurrent = counter.getAndDecrement();
				if (concurrent > maxConcurrentBatches) {
					errors.incrementAndGet();
				}
				batch.forEach(e -> e.success(concurrent));
			}, 10, TimeUnit.MILLISECONDS);
		}).run();

		assertThat(errors).hasValue(0);
	}


	@Test(timeout = 20_100)
	public void verifyEmptyBatch() {
		final BatchQueue<Integer, Integer> queue = new BatchQueue<>(1, 1, TimeUnit.SECONDS);
		queue.shutdown();

		BatchRunnerFactory.forConsumer(queue, 2, 5, 1, TimeUnit.SECONDS, batch -> fail("There is no batch.")).run();
	}


	@Test
	public void verifyMultiplexingWithFullResultsWorks() {
		final Consumer<List<BatchElement<String, String>>> batchConsumer = BatchRunnerFactory.multiplexOverFunction(Function.identity(),
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
	public void verifyMultiplexingWithPartialResultsWorks() {
		final Consumer<List<BatchElement<String, String>>> batchConsumer = BatchRunnerFactory.multiplexOverFunction(Function.identity(),
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
		assertThat(e3.outputFuture).hasFailedWithThrowableThat().isInstanceOf(IllegalStateException.class);
		assertThat(e4.outputFuture).isCompletedWithValue("[four]");
		assertThat(e5.outputFuture).isCompletedWithValue("[five]");
	}


	@Test
	public void verifyMultiplexingWithFailureWorks() {
		final RuntimeException exception = new RuntimeException();
		final Consumer<List<BatchElement<String, String>>> batchConsumer = BatchRunnerFactory.multiplexOverFunction(Function.identity(),
			CompletableFuture::completedFuture, (batch, results) -> {
				throw exception;
			});
		batchConsumer.accept(asList(e1, e2, e3, e4, e5));

		assertThat(e1.outputFuture).hasFailedWithThrowableThat().isSameAs(exception);
		assertThat(e2.outputFuture).hasFailedWithThrowableThat().isSameAs(exception);
		assertThat(e3.outputFuture).hasFailedWithThrowableThat().isSameAs(exception);
		assertThat(e4.outputFuture).hasFailedWithThrowableThat().isSameAs(exception);
		assertThat(e5.outputFuture).hasFailedWithThrowableThat().isSameAs(exception);
	}
}
