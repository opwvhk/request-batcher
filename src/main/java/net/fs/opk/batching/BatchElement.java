package net.fs.opk.batching;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


/**
 * An element in a batch. Used for {@link BatchQueue} and {@link BatchRunner}.
 *
 * @param <T>
 * @param <R>
 */
public class BatchElement<T, R> {
	private final long deadlineNanos;
	private final T inputValue;
	final CompletableFuture<R> outputFuture;


	BatchElement(final long deadlineNanos, final T inputValue) {
		this.deadlineNanos = deadlineNanos;
		this.inputValue = inputValue;
		this.outputFuture = new CompletableFuture<>();
	}


	/**
	 * The value of {@link System#nanoTime()} up to which this element may await more elements to batch.
	 *
	 * @return the linger deadline
	 */
	long getDeadlineNanos() {
		return deadlineNanos;
	}


	/**
	 * Get the value that was inputted in the batch.
	 *
	 * @return the batched input value
	 */
	public T getInputValue() {
		return inputValue;
	}


	/**
	 * Report a value for this batch element. Calling this method successfully completes the {@link CompletableFuture CompletableFuture&lt;R&gt;} returned by
	 * {@link BatchQueue#enqueue(Object, long, TimeUnit) BatchQueue&lt;T, R&gt;#enqueue(T, long, TimeUnit)} or
	 * {@link BatchQueue#enqueue(Object) BatchQueue&lt;T, R&gt;#enqueue(T)}.
	 *
	 * @param result the result of the batched request for this element
	 */
	public void success(final R result) {
		outputFuture.complete(result);
	}


	/**
	 * Report an error for this batch element. Calling this method completes the {@link CompletableFuture CompletableFuture&lt;R&gt;} returned by
	 * {@link BatchQueue#enqueue(Object, long, TimeUnit) BatchQueue&lt;T, R&gt;#enqueue(T, long, TimeUnit)} or
	 * {@link BatchQueue#enqueue(Object) BatchQueue&lt;T, R&gt;#enqueue(T)} exceptionally.
	 *
	 * @param error the error encountered while handling the batched request for this element
	 */
	public void error(final Throwable error) {
		outputFuture.completeExceptionally(error);
	}
}
