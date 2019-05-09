package net.fs.opk.batching;

import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;


public class BatchElementTest {
	private long deadlineNanos;
	private Integer inputValue;
	private Integer outputValue;


	@Before
	public void setUp() {
		final Random random = new Random();
		deadlineNanos = random.nextLong();
		inputValue = random.nextInt();
		outputValue = random.nextInt();
	}


	@Test
	public void verifySucces() {
		final BatchElement<Integer, Integer> element = new BatchElement<>(deadlineNanos, inputValue);
		assertThat(element.getDeadlineNanos()).isEqualTo(deadlineNanos);
		assertThat(element.getInputValue()).isEqualTo(inputValue);
		assertThat(element.outputFuture).isNotDone();

		element.success(outputValue);

		assertThat(element.outputFuture).isCompletedWithValue(outputValue);
	}


	@Test
	public void verifyFailure() {
		final BatchElement<Integer, Integer> element = new BatchElement<>(deadlineNanos, inputValue);
		assertThat(element.getDeadlineNanos()).isEqualTo(deadlineNanos);
		assertThat(element.getInputValue()).isEqualTo(inputValue);
		assertThat(element.outputFuture).isNotDone();

		final RuntimeException error = new RuntimeException("test error");
		element.error(error);

		assertThat(element.outputFuture).hasFailedWithThrowableThat().isSameAs(error).hasMessage("test error");
	}
}
