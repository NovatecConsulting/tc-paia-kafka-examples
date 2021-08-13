package de.novatec.tc.pseudonymize.generic;

import de.novatec.tc.account.v1.Account;
import de.novatec.tc.support.*;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import static de.novatec.tc.support.FileSupport.deleteCloseable;
import static de.novatec.tc.support.FileSupport.tempDirectory;
import static java.util.UUID.randomUUID;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.apache.kafka.streams.state.Stores.keyValueStoreBuilder;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;

public class PseudonymizeApp {

    public static void main(final String[] args) {
        final AppConfigs appConfigs = AppConfigs.fromAll(
                AppConfigs.fromResource("pseudonymize-generic.properties"),
                AppConfigs.fromEnv("APP_"),
                AppConfigs.fromArgs(args)).doLog();

        new PseudonymizeApp()
                .runApp(appConfigs)
                .registerShutdownHook(Duration.ofSeconds(10));
    }

    public StreamsApp runApp(final AppConfigs appConfigs) {
        new TopicSupport(appConfigs.createMap())
                .createTopicsIfNotExists(appConfigs.topics(), Duration.ofSeconds(5));

        final Collection<AutoCloseable> closeables = new CopyOnWriteArrayList<>();
        final AppConfigs actualAppConfigs = appConfigs.createAppConfigsWith(newConfigs ->
            newConfigs.computeIfAbsent(STATE_DIR_CONFIG,
                    key -> deleteCloseable(tempDirectory("kafka-streams"), closeables::add).getPath())
        );

        final KafkaStreams streams = new KafkaStreams(buildTopology(actualAppConfigs), actualAppConfigs.createProperties());
        streams.start();

        return new StreamsApp(streams, closeables);
    }

    public Topology buildTopology(final AppConfigs appConfigs) {
        final SerdeBuilder<String> stringSerdeBuilder = SerdeBuilder.fromSerdeSupplier(Serdes.StringSerde::new);
        final SerdeBuilder<GenericRecord> eventSerdeBuilder = SerdeBuilder.fromSerdeSupplier(GenericAvroSerde::new);
        final SerdeBuilder<Account> accountSerdeBuilder = SerdeBuilder.fromSerdeSupplier(SpecificAvroSerde::new);
        return buildTopology(appConfigs, stringSerdeBuilder, eventSerdeBuilder, accountSerdeBuilder);
    }

    public Topology buildTopology(final AppConfigs appConfigs,
                                  final SerdeBuilder<String> stingSerdeBuilder,
                                  final SerdeBuilder<GenericRecord> eventSerdeBuilder,
                                  final SerdeBuilder<Account> accountSerdeBuilder) {
        final Serde<String> stringSerde = stingSerdeBuilder.build(appConfigs.createMap(), true);
        final Serde<GenericRecord> actionEventSerde = eventSerdeBuilder.build(appConfigs.createMap(), false);
        final Serde<Account> accountSerde = accountSerdeBuilder.build(appConfigs.createMap(), false);

        final StreamsBuilder builder = new StreamsBuilder();

        final StoreBuilder<KeyValueStore<String, Account>> pseudonymStoreBuilder =
                keyValueStoreBuilder(persistentKeyValueStore(appConfigs.storeName("pseudonym")), stringSerde, accountSerde);
        builder.addStateStore(pseudonymStoreBuilder);

        // create a stream for each configured input/output topic
        // the streams are linked by the state store, which ensures, that the partitions ar co-located
        for (AppConfigs streamConfigs : appConfigs.withPrefix("stream").allSubAppConfigs()) {
            builder.stream(streamConfigs.topicName("input"), Consumed.with(stringSerde, actionEventSerde))
                    .transform(() -> new PseudonymizeTransformer(TypedStoreRef.fromBuilder(pseudonymStoreBuilder)), pseudonymStoreBuilder.name())
                    .to(streamConfigs.topicName("output"), Produced.with(stringSerde, actionEventSerde));
        }

        return builder.build();
    }

    public static class PseudonymizeTransformer implements Transformer<String, GenericRecord, KeyValue<String, GenericRecord>> {

        private final TypedStoreRef<KeyValueStore<String, Account>> pseudonymStoreRef;

        private KeyValueStore<String, Account> pseudonymStore;

        public PseudonymizeTransformer(final TypedStoreRef<KeyValueStore<String, Account>> pseudonymStoreRef) {
            this.pseudonymStoreRef = pseudonymStoreRef;
        }

        @Override
        public void init(final ProcessorContext context) {
            // KafkaStreams creates a StateStore for each Task.
            // Therefore, the actual StateStore must be retrieved from the context.
            // The TypedStoreRef basically wraps the following action:
            //   pseudonymStore = context.getStateStore(pseudonymStoreName);
            pseudonymStore = pseudonymStoreRef.getStateStore(context);
        }

        @Override
        public KeyValue<String, GenericRecord> transform(final String accountId, final GenericRecord event) {
            Account pseudonymAccount = pseudonymStore.get(accountId);
            if (pseudonymAccount == null) {
                pseudonymAccount = Account.newBuilder()
                        .setAccountId(randomUUID().toString())
                        .setAccountName(randomAlphabetic(5, 10))
                        .build();
                pseudonymStore.put(accountId, pseudonymAccount);
            }
            final GenericRecord pseudonymizedEvent;
            if (event != null) {
                pseudonymizedEvent = GenericData.get().deepCopy(event.getSchema(), event);
                pseudonymizedEvent.put("eventId", randomUUID().toString());
                pseudonymizedEvent.put("account", Account.newBuilder(pseudonymAccount).build());
            } else {
                pseudonymizedEvent = null;
            }
            return new KeyValue<>(pseudonymAccount.getAccountId(), pseudonymizedEvent);
        }

        @Override
        public void close() {}

    }
}
