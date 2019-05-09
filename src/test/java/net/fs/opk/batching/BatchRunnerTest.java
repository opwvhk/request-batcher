package net.fs.opk.batching;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;


public class BatchRunnerTest {
	private static final int BATCH_SIZE = 3;
	private static final long BATCH_TIMEOUT_MS = 100;

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
			assertThat((TimeUnit)invocation.getArgument(1)).isEqualTo(TimeUnit.MILLISECONDS);
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

		runner = new BatchRunner<String, String>(queue, BATCH_SIZE, BATCH_TIMEOUT_MS) {
			@Override
			protected void executeBatch(final List<BatchElement<String, String>> batch) {
				batchHandler.accept(batch);
			}
		};
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

		verify(queue, times(3)).acquireBatch(anyLong(), eq(TimeUnit.MILLISECONDS), anyInt(), anyCollection());

		assertThat(e1.outputFuture).isCompletedWithValue("[one]");
		assertThat(e2.outputFuture).isCompletedWithValue("[two]");
		assertThat(e3.outputFuture).isCompletedWithValue("[three]");
		assertThat(e4.outputFuture).isCompletedWithValue("[four]");
		assertThat(e5.outputFuture).isCompletedWithValue("[five]");
	}


	@Test(timeout = 300)
	public void validateNonResponse() throws InterruptedException {

		runner.run();

		verify(queue, times(3)).acquireBatch(anyLong(), eq(TimeUnit.MILLISECONDS), anyInt(), anyCollection());

		assertThat(e1.outputFuture).hasFailedWithThrowableThat().isInstanceOf(TimeoutException.class);
		assertThat(e2.outputFuture).hasFailedWithThrowableThat().isInstanceOf(TimeoutException.class);
		assertThat(e3.outputFuture).hasFailedWithThrowableThat().isInstanceOf(TimeoutException.class);
		assertThat(e4.outputFuture).hasFailedWithThrowableThat().isInstanceOf(TimeoutException.class);
		assertThat(e5.outputFuture).hasFailedWithThrowableThat().isInstanceOf(TimeoutException.class);
	}


	@Test(timeout = 50)
	public void validateTermination() throws InterruptedException {
		when(queue.acquireBatch(anyLong(), any(TimeUnit.class), anyInt(), anyCollection())).thenThrow(new InterruptedException());

		runner.run();
	}


	@Test(timeout = 50)
	public void validateAbnormalTermination() throws InterruptedException {
		final RuntimeException exception = new RuntimeException();
		when(queue.acquireBatch(anyLong(), any(TimeUnit.class), anyInt(), anyCollection())).thenThrow(exception);

		assertThatThrownBy(runner::run).isSameAs(exception);
	}


	@Test
	public void verifyLambdaRunWithPartialResult() {
		BatchRunner.forLambdas(queue, BATCH_SIZE, BATCH_TIMEOUT_MS, Function.identity(), CompletableFuture::completedFuture, (batch, results) -> {
			for (int i = 0; i < batch.size(); i++) {
				if (i == 2) {
					continue;
				}
				results.get(i).success("[" + batch.get(i) + "]");
			}
		}).run();

		assertThat(e1.outputFuture).isCompletedWithValue("[one]");
		assertThat(e2.outputFuture).isCompletedWithValue("[two]");
		assertThat(e3.outputFuture).hasFailedWithThrowableThat().isInstanceOf(IllegalStateException.class);
		assertThat(e4.outputFuture).isCompletedWithValue("[four]");
		assertThat(e5.outputFuture).isCompletedWithValue("[five]");
	}


	@Test
	public void verifyLambdaRunWithFailure() {
		final RuntimeException exception = new RuntimeException();
		BatchRunner.forLambdas(queue, BATCH_SIZE, BATCH_TIMEOUT_MS, Function.identity(), CompletableFuture::completedFuture, (batch, results) -> {
			throw exception;
		}).run();

		assertThat(e1.outputFuture).hasFailedWithThrowableThat().isSameAs(exception);
		assertThat(e2.outputFuture).hasFailedWithThrowableThat().isSameAs(exception);
		assertThat(e3.outputFuture).hasFailedWithThrowableThat().isSameAs(exception);
		assertThat(e4.outputFuture).hasFailedWithThrowableThat().isSameAs(exception);
		assertThat(e5.outputFuture).hasFailedWithThrowableThat().isSameAs(exception);
	}
}
