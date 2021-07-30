package de.novatec.tc.support;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;

public interface TypedStoreRef<S extends StateStore> {

    S getStateStore(ProcessorContext context);

    static <S extends StateStore> TypedStoreRef<S> fromBuilder(final StoreBuilder<S> storeBuilder) {
        return context -> context.getStateStore(storeBuilder.name());
    }
}