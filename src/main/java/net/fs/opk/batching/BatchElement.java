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
	/**
	 * The value of {@link System#nanoTime()} up to which this element may await more elements to batch.
	 */
	final long lingerDeadlineNanos;
	private final T inputValue;
	final CompletableFuture<R> outputFuture;


	BatchElement(long lingerDeadlineNanos, long completionDeadlineNanos, T inputValue) {
		this.lingerDeadlineNanos = lingerDeadlineNanos;
		this.inputValue = inputValue;
		this.outputFuture = new CompletableFuture<R>().orTimeout(completionDeadlineNanos, TimeUnit.NANOSECONDS);
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
	public void success(R result) {
		outputFuture.complete(result);
	}


	/**
	 * Report an error for this batch element. Calling this method completes the {@link CompletableFuture CompletableFuture&lt;R&gt;} returned by
	 * {@link BatchQueue#enqueue(Object, long, TimeUnit) BatchQueue&lt;T, R&gt;#enqueue(T, long, TimeUnit)} or
	 * {@link BatchQueue#enqueue(Object) BatchQueue&lt;T, R&gt;#enqueue(T)} exceptionally.
	 *
	 * @param error the error encountered while handling the batched request for this element
	 */
	public void error(Throwable error) {
		outputFuture.completeExceptionally(error);
	}


	/**
	 * Report a result for this batch element. Reports success unless {@code error != null}.
	 *
	 * @param result the (eventual) result for the batch element
	 */
	public void report(R result, Throwable error) {
		if (error == null) {
			outputFuture.complete(result);
		} else {
			outputFuture.completeExceptionally(error);
		}
	}
}
