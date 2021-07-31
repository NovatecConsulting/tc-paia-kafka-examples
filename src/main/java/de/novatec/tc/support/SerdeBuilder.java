package de.novatec.tc.support;

import org.apache.kafka.common.serialization.Serde;

import java.util.Map;
import java.util.function.Supplier;

public interface SerdeBuilder<T> {

    Serde<T> build(Map<String, ?> configs, Boolean isKey);

    static <T> SerdeBuilder<T>  fromSerdeSupplier(final Supplier<Serde<T>> serdeSupplier) {
        return (configs, isKey) -> {
            final Serde<T> serde = serdeSupplier.get();
            serde.configure(configs, isKey);
            return serde;
        };
    }
}
