package net.fs.opk.batching;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


class BatchElementTest {
	private static final long NANOS_FOR_365_DAYS = 31536000000000000L;
	private long lingerDeadlineNanos;
	private long completionDeadlineNanos;
	private Integer inputValue;
	private Integer outputValue;
	private RuntimeException error;


	@BeforeEach
	void setUp() {
		Random random = new Random();
		lingerDeadlineNanos = random.nextLong(0, NANOS_FOR_365_DAYS);
		completionDeadlineNanos = random.nextLong(lingerDeadlineNanos, NANOS_FOR_365_DAYS);
		inputValue = random.nextInt();
		outputValue = random.nextInt();
		error = new RuntimeException("test error " + random.nextInt());
	}


	@Test
	void verifySimpleFields() {
		BatchElement<Integer, Integer> element = new BatchElement<>(lingerDeadlineNanos, completionDeadlineNanos, inputValue);
		assertThat(element.lingerDeadlineNanos).isEqualTo(lingerDeadlineNanos);
		assertThat(element.getInputValue()).isEqualTo(inputValue);
		assertThat(element.outputFuture).isNotDone();
	}


	@Test
	void verifySuccess() {
		BatchElement<Integer, Integer> element = new BatchElement<>(lingerDeadlineNanos, completionDeadlineNanos, inputValue);

		element.success(outputValue);
		assertThat(element.outputFuture).isCompletedWithValue(outputValue);
	}


	@Test
	void verifyFailure() {
		BatchElement<Integer, Integer> element = new BatchElement<>(lingerDeadlineNanos, completionDeadlineNanos, inputValue);

		element.error(error);
		assertThat(element.outputFuture)
			.failsWithin(Duration.ZERO)
			.withThrowableOfType(Exception.class) // We only case about the cause, not the wrapping exception
			.withCause(error);
	}


	@Test
	void verifySuccessForCompletableFutures() {
		BatchElement<Integer, Integer> element = new BatchElement<>(lingerDeadlineNanos, completionDeadlineNanos, inputValue);
		CompletableFuture.completedFuture(outputValue).whenComplete(element::report);

		assertThat(element.outputFuture).isCompletedWithValue(outputValue);
	}


	@Test
	void verifyFailureForCompletableFutures() {
		BatchElement<Integer, Integer> element = new BatchElement<>(lingerDeadlineNanos, completionDeadlineNanos, inputValue);
		CompletableFuture.<Integer>failedFuture(error).whenComplete(element::report);

		assertThat(element.outputFuture)
			.failsWithin(Duration.ZERO)
			.withThrowableOfType(Exception.class) // We only case about the cause, not the wrapping exception
			.withCause(error);
	}
}
