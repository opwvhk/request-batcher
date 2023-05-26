package net.fs.opk.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.*;

class NamerTest {
	@Test
	void testNameAsPassed() {
		Namer namer = new Namer("name_");
		assertThat(namer.name("foo")).isEqualTo("foo");
	}

	@Test
	void testGeneratedNames() {
		Namer namer = new Namer("name_");
		assertThat(namer.name(null)).isEqualTo("name_1");
		assertThat(namer.name(null)).isEqualTo("name_2");
		assertThat(namer.name(null)).isEqualTo("name_3");
	}

	@Test
	void testPrefixSanitation() {
		Namer namer = new Namer(" ná -Me _");
		assertThat(namer.name(null)).isEqualTo("na_me_1");
		assertThat(namer.name(null)).isEqualTo("na_me_2");
		assertThat(namer.name(null)).isEqualTo("na_me_3");
	}
}
