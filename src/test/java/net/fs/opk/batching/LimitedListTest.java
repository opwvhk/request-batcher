package net.fs.opk.batching;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class LimitedListTest {

	private static final int CAPACITY = 3;
	private static final String ONE = "one";
	private static final String TWO = "two";
	private static final String THREE = "three";
	private static final String FOUR = "four";


	@Test
	public void verifyAddAndGet() {
		final LimitedList<String> list = new LimitedList<>(CAPACITY);

		assertThat(list.add(ONE)).isTrue();
		assertThat(list.add(TWO)).isTrue();
		assertThat(list.add(THREE)).isTrue();

		assertThat(list.get(0)).isEqualTo(ONE);
		assertThat(list.get(1)).isEqualTo(TWO);
		assertThat(list.get(2)).isEqualTo(THREE);
	}


	@Test
	public void verifyLimit() {
		final LimitedList<String> list = new LimitedList<>(CAPACITY);

		assertThat(list.add(ONE)).isTrue();
		assertThat(list.add(TWO)).isTrue();
		assertThat(list.add(THREE)).isTrue();
		assertThatThrownBy(() -> list.add(FOUR)).isInstanceOf(IllegalStateException.class);
	}


	@Test
	public void verifyBounds() {
		//noinspection MismatchedQueryAndUpdateOfCollection
		final LimitedList<String> list = new LimitedList<>(CAPACITY);
		list.add(ONE);
		list.add(TWO);

		//noinspection ResultOfMethodCallIgnored,ConstantConditions
		assertThatThrownBy(() -> list.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);
		//noinspection ResultOfMethodCallIgnored
		assertThatThrownBy(() -> list.get(2)).isInstanceOf(IndexOutOfBoundsException.class);
	}


	@Test
	public void verifyClearAndSize() {
		final LimitedList<String> list = new LimitedList<>(CAPACITY);
		list.add(ONE);
		list.add(TWO);

		assertThat(list.size()).isEqualTo(2);
		assertThat(list.capacity()).isEqualTo(CAPACITY);

		list.clear();

		assertThat(list.size()).isEqualTo(0);
		assertThat(list.capacity()).isEqualTo(CAPACITY);
	}
}
