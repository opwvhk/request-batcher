package net.fs.opk.batching;

import java.util.AbstractList;
import java.util.Arrays;


/**
 * A capacity limited list, backed by an array, only supporting appending elements and clearing the entire list.
 *
 * @param <E> the element type
 */
class LimitedList<E> extends AbstractList<E> {

	/**
	 * The elements in the list.
	 */
	private final Object[] elements;
	/**
	 * The current number of elements.
	 */
	private int count;


	public LimitedList(final int capacity) {
		elements = new Object[capacity];
		count = 0;
	}


	@Override
	public boolean add(final E element) {
		if (count == elements.length) {
			throw new IllegalStateException("No capacity left");
		}
		elements[count++] = element;
		return true;
	}


	@Override
	public E get(final int index) {
		if (index < 0 || index >= size()) {
			throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + count);
		}
		return (E)elements[index];
	}


	@Override
	public void clear() {
		Arrays.fill(elements, null);
		count = 0;
	}


	@Override
	public int size() {
		return count;
	}


	public int capacity() {
		return elements.length;
	}
}
