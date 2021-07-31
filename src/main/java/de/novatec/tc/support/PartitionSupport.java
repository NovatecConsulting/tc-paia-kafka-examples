package de.novatec.tc.support;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Utils;

import java.util.function.Function;

public class PartitionSupport {

    public <T> int partition(final T key, final Serializer<T> serializer, final String topic, final int numPartitions) {
        return partition(serializer.serialize(topic, key), numPartitions);
    }

    public <T> int partition(final T key, final Function<T, byte[]> serializer, final int numPartitions) {
        return partition(serializer.apply(key), numPartitions);
    }

    public int partition(final byte[] keyBytes, final int numPartitions) {
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }
}
