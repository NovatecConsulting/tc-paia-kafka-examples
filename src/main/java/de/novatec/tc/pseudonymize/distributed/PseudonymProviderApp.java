package de.novatec.tc.pseudonymize.distributed;

import de.novatec.tc.account.v1.Account;
import de.novatec.tc.account.v1.CreatePseudonymAccountCommand;
import de.novatec.tc.account.v1.PseudonymAccountEvent;
import de.novatec.tc.support.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
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
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;

import static de.novatec.tc.support.FileSupport.deleteCloseable;
import static de.novatec.tc.support.FileSupport.tempDirectory;
import static java.util.UUID.randomUUID;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.apache.kafka.streams.state.Stores.keyValueStoreBuilder;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;

public class PseudonymProviderApp {

    public static void main(final String[] args) {
        final AppConfigs appConfigs = AppConfigs.fromAll(
                AppConfigs.fromResource("pseudonymize-distributed-provider.properties"),
                AppConfigs.fromEnv("APP_"),
                AppConfigs.fromArgs(args)).doLog();

        new PseudonymProviderApp()
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
        final SerdeBuilder<CreatePseudonymAccountCommand> commandSerdeBuilder = SerdeBuilder.fromSerdeSupplier(SpecificAvroSerde::new);
        final SerdeBuilder<PseudonymAccountEvent> eventSerdeBuilder = SerdeBuilder.fromSerdeSupplier(SpecificAvroSerde::new);
        return buildTopology(appConfigs, stringSerdeBuilder, commandSerdeBuilder, eventSerdeBuilder);
    }

    public Topology buildTopology(final AppConfigs appConfigs,
                                  final SerdeBuilder<String> stingSerdeBuilder,
                                  final SerdeBuilder<CreatePseudonymAccountCommand> commandSerdeBuilder,
                                  final SerdeBuilder<PseudonymAccountEvent> eventSerdeBuilder) {
        final Serde<String> stringSerde = stingSerdeBuilder.build(appConfigs.createMap(), true);
        final Serde<CreatePseudonymAccountCommand> commandSerde = commandSerdeBuilder.build(appConfigs.createMap(), false);
        final Serde<PseudonymAccountEvent> eventSerde = eventSerdeBuilder.build(appConfigs.createMap(), false);

        final StreamsBuilder builder = new StreamsBuilder();

        final StoreBuilder<KeyValueStore<String, PseudonymAccountEvent>> pseudonymStoreBuilder =
                keyValueStoreBuilder(persistentKeyValueStore(appConfigs.storeName("pseudonymaccount")), stringSerde, eventSerde)
                        .withCachingDisabled();
        builder.addStateStore(pseudonymStoreBuilder);

        builder.stream(appConfigs.topicName("input"), Consumed.with(stringSerde, commandSerde))
                .transform(() -> new PseudonymProvider(TypedStoreRef.fromBuilder(pseudonymStoreBuilder)), pseudonymStoreBuilder.name())
                .to(appConfigs.topicName("output"), Produced.with(stringSerde, eventSerde));

        return builder.build();
    }

    public static class PseudonymProvider implements Transformer<String, CreatePseudonymAccountCommand, KeyValue<String, PseudonymAccountEvent>> {

        private final TypedStoreRef<KeyValueStore<String, PseudonymAccountEvent>> pseudonymStoreRef;

        private KeyValueStore<String, PseudonymAccountEvent> pseudonymStore;

        public PseudonymProvider(final TypedStoreRef<KeyValueStore<String, PseudonymAccountEvent>> pseudonymStoreRef) {
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
        public KeyValue<String, PseudonymAccountEvent> transform(final String accountId, final CreatePseudonymAccountCommand event) {
            PseudonymAccountEvent pseudonymAccountEvent = pseudonymStore.get(accountId);
            if (pseudonymAccountEvent == null) {
                pseudonymAccountEvent = PseudonymAccountEvent.newBuilder()
                        .setEventId(randomUUID().toString())
                        .setEventTime(Instant.now())
                        .setAccount(Account.newBuilder()
                                .setAccountId(randomUUID().toString())
                                .setAccountName(randomAlphabetic(5, 10)).build()).build();
                pseudonymStore.put(accountId, pseudonymAccountEvent);
                return new KeyValue<>(accountId, pseudonymAccountEvent);
            }
            return null;
        }

        @Override
        public void close() {}

    }
}
