package de.novatec.tc.support;

import java.time.Duration;

public interface Stoppable extends AutoCloseable {

    void close(Duration timeout);

    @Override
    default void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    default Stoppable registerShutdownHook(Duration timeout) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> close(timeout), "stoppable-shutdown-hook"));
        return this;
    }

    default Stoppable registerShutdownHook() {
        return registerShutdownHook(Duration.ofMillis(Long.MAX_VALUE));
    }
}
