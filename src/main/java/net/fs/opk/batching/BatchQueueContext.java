package net.fs.opk.batching;

import io.micrometer.observation.Observation;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class BatchQueueContext<Request, Response> extends Observation.Context {
	private BatchQueue<Request, Response> batchQueue;
	private Request request;
	private Response response;

	public BatchQueueContext(BatchQueue<Request, Response> batchQueue, Request request) {
		this.batchQueue = batchQueue;
		this.request = request;
	}
}
