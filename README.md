# Request Batcher

For many systems, batching requests together amplifies throughput. This library takes care of the basics when batching multiple requests together. All you have to do is handle the batched requests.


## Usage example

In this (contrived) example, we'll batch number requests, and add the number of elements in the batch.

````java
import com.bol.measure.environment.EnvironmentConfig;
import com.bol.measure.environment.StreamEnvironmentFactory;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.net.URI;

public class Example {
    public static void main(final String[] args) {
        // Note: there are more configuration options in the interface (and you can change the environment as well)
        final EnvironmentConfig config = new EnvironmentConfig() {
            public long checkpointIntervalMillis() {
                // Every minute
                return 60_000L;
            }

            public URI backendURI() {
                return URI.create("hdfs:///tmp/stateBackend");
            }
        };

        final StreamExecutionEnvironment env = StreamEnvironmentFactory.create(config);

        // ...
    }
}
````


