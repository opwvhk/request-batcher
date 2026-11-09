package net.fs.opk.batching;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import org.jspecify.annotations.NullMarked;

@NullMarked
public class DefaultBatchQueueObservationConvention<Request, Response>
	implements ObservationConvention<BatchQueueContext<Request, Response>> {

	@Override
	public boolean supportsContext(Observation.Context context) {
		return context instanceof BatchQueueContext;
	}

	@Override
	public KeyValues getLowCardinalityKeyValues(BatchQueueContext<Request, Response> context) {
		String name = context.getBatchQueue().name();
		return KeyValues.of(LowCardinalityKeyValues.queue(name));
	}

	public static class LowCardinalityKeyValues {
		public static KeyValue queue(String value) {
			return KeyValue.of("queue", value);
		}
	}
}
