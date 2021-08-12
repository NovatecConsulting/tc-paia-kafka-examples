package de.novatec.tc.support;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;

public class StreamsApp implements Stoppable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamsApp.class);

    private final KafkaStreams streams;
    private final Collection<AutoCloseable> closeables;

    private final AtomicBoolean initialized = new AtomicBoolean(false);

    public StreamsApp(KafkaStreams streams, Collection<AutoCloseable> closeables) {
        this.streams = streams;
        this.closeables = closeables;
    }

    public boolean isInitialized() {
        return initialized.compareAndExchange(false, isRunning());
    }

    public boolean isRunning() {
        return streams.state().equals(RUNNING);
    }

    @Override
    public void close(Duration timeout) {
        LOG.info(format("Initiating shutdown of %s", this.getClass().getSimpleName()));
        for (int i = 0; i < streams.metadataForLocalThreads().size(); i++) {
            streams.removeStreamThread(); // leave consumer group
        }
        if (streams.close(timeout)) {
            closeables.forEach(c -> closeQuietly(c, c.getClass().getSimpleName()));
        }
        LOG.info(format("Shutdown of %s completed", this.getClass().getSimpleName()));
    }

}
