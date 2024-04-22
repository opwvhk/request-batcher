package net.fs.opk.util;

import java.text.Normalizer;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Simple utility to name stuff. Ensures names exist by either taking a given name, or by creating one. Created names
 * are numbered to ensure uniqueness.
 */
public class Namer {
	private final String defaultName;
	private final AtomicInteger counter;
	private final Function<String, String> nameCorrector;

	/**
	 * Create a namer using the specified default name. Equivalent to {@code new Namer(defaultName, String::strip)}.
	 *
	 * @param defaultName the default name to use
	 */
	public Namer(String defaultName) {
		this(defaultName, Namer::sanitize);
	}

	private static String sanitize(String prefix) {
		// Remove all accents, collapse non-word characters and underscores into single underscores and convert to lowercase.
		return Normalizer.normalize(prefix.strip(), Normalizer.Form.NFKD).replaceAll("\\p{M}", "")
			.replaceAll("[\\W_]+", "_").toLowerCase(Locale.ROOT);
	}

	/**
	 * Create a namer using the specified default name, and a function to correct names (e.g., by removing whitespace,
	 * fixing capitalization, etc.).
	 *
	 * @param defaultName   the default name to use
	 * @param nameCorrector a function that corrects names before returning them
	 */
	public Namer(String defaultName, Function<String, String> nameCorrector) {
		this.defaultName = nameCorrector.apply(requireNonNull(defaultName));
		this.nameCorrector = nameCorrector;
		counter = new AtomicInteger(1);
	}

	public String name(String name) {
		return name == null ? defaultName + counter.getAndIncrement() : nameCorrector.apply(name);
	}
}
