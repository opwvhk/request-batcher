package net.fs.opk.batching;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


/**
 * An element in a batch. Used for {@link BatchQueue} and {@link BatchRunner}.
 *
 * @param <T> the type of the input value
 * @param <R> the type of the result value
 */
public class BatchElement<T, R> {
	/**
	 * The value of {@link System#nanoTime()} up to which this element may await more elements to batch.
	 */
	final long lingerDeadlineNanos;
	private final T inputValue;
	final CompletableFuture<R> outputFuture;


	BatchElement(T inputValue, long lingerDeadlineNanos, long completionTimeoutNanos) {
		this.lingerDeadlineNanos = lingerDeadlineNanos;
		this.inputValue = inputValue;
		this.outputFuture = new CompletableFuture<R>().orTimeout(completionTimeoutNanos, TimeUnit.NANOSECONDS);
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
	 * Report a value for this batch element.
	 *
	 * <p>Calling this method completes the {@link CompletableFuture CompletableFuture&lt;R&gt;} returned by
	 * {@link BatchQueue#enqueue(Object, long, TimeUnit) BatchQueue&lt;T, R&gt;#enqueue(T, long, TimeUnit)} or
	 * {@link BatchQueue#enqueue(Object) BatchQueue&lt;T, R&gt;#enqueue(T)} successfully with the specified value.</p>
	 *
	 * @param result the result of the batched request for this element
	 */
	public void success(R result) {
		report(result, null);
	}


	/**
	 * Report an error for this batch element.
	 *
	 * <p>Calling this method completes the {@link CompletableFuture CompletableFuture&lt;R&gt;} returned by
	 * {@link BatchQueue#enqueue(Object, long, TimeUnit) BatchQueue&lt;T, R&gt;#enqueue(T, long, TimeUnit)} or
	 * {@link BatchQueue#enqueue(Object) BatchQueue&lt;T, R&gt;#enqueue(T)} with the given failure.</p>
	 *
	 * @param error the error encountered while handling the batched request for this element
	 */
	public void error(Throwable error) {
		report(null, error);
	}


	/**
	 * <p>Report a result for this batch element. Reports success unless {@code error != null}.</p>
	 *
	 * <p>Calling this method completes the {@link CompletableFuture CompletableFuture&lt;R&gt;} returned by
	 * {@link BatchQueue#enqueue(Object, long, TimeUnit) BatchQueue&lt;T, R&gt;#enqueue(T, long, TimeUnit)} or
	 * {@link BatchQueue#enqueue(Object) BatchQueue&lt;T, R&gt;#enqueue(T)}.</p>
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
