package net.fs.opk.batching;

import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;


public class BatchElementTest {
	private long deadlineNanos;
	private Integer inputValue;
	private Integer outputValue;
	private RuntimeException error;


	@Before
	public void setUp() {
		final Random random = new Random();
		deadlineNanos = random.nextLong();
		inputValue = random.nextInt();
		outputValue = random.nextInt();
		error = new RuntimeException("test error " + random.nextInt());
	}


	@Test
	public void verifySimpleFields() {
		final BatchElement<Integer, Integer> element = new BatchElement<>(deadlineNanos, inputValue);
		assertThat(element.getDeadlineNanos()).isEqualTo(deadlineNanos);
		assertThat(element.getInputValue()).isEqualTo(inputValue);
		assertThat(element.outputFuture).isNotDone();
	}


	@Test
	public void verifySuccess() {
		final BatchElement<Integer, Integer> element = new BatchElement<>(deadlineNanos, inputValue);

		element.success(outputValue);
		assertThat(element.outputFuture).isCompletedWithValue(outputValue);
	}


	@Test
	public void verifyFailure() {
		final BatchElement<Integer, Integer> element = new BatchElement<>(deadlineNanos, inputValue);

		element.error(error);
		assertThat(element.outputFuture).hasFailedWithThrowableThat().isInstanceOf(RuntimeException.class).hasMessage(error.getMessage());
	}


	@Test
	public void verifyEventualSuccess() {
		final BatchElement<Integer, Integer> element = new BatchElement<>(deadlineNanos, inputValue);
		element.report(CompletableFuture.completedFuture(outputValue));

		assertThat(element.outputFuture).isCompletedWithValue(outputValue);
	}


	@Test
	public void verifyEventualFailure() {
		final BatchElement<Integer, Integer> element = new BatchElement<>(deadlineNanos, inputValue);
		final CompletableFuture<Integer> future = new CompletableFuture<>();
		future.completeExceptionally(error);
		element.report(future);

		assertThat(element.outputFuture).hasFailedWithThrowableThat().isInstanceOf(RuntimeException.class).hasMessage(error.getMessage());
	}
}
