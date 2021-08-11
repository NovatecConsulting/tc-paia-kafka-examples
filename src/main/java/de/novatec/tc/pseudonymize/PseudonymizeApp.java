package de.novatec.tc.pseudonymize;

import de.novatec.tc.account.v1.Account;
import de.novatec.tc.account.v1.ActionEvent;
import de.novatec.tc.support.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
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
                AppConfigs.fromResource("pseudonymize.properties"),
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
        final SerdeBuilder<ActionEvent> actionEventSerdeBuilder = SerdeBuilder.fromSerdeSupplier(SpecificAvroSerde::new);
        final SerdeBuilder<Account> accountSerdeBuilder = SerdeBuilder.fromSerdeSupplier(SpecificAvroSerde::new);
        return buildTopology(appConfigs, stringSerdeBuilder, actionEventSerdeBuilder, accountSerdeBuilder);
    }

    public Topology buildTopology(final AppConfigs appConfigs,
                                  final SerdeBuilder<String> stingSerdeBuilder,
                                  final SerdeBuilder<ActionEvent> actionEventSerdeBuilder,
                                  final SerdeBuilder<Account> accountSerdeBuilder) {
        final Serde<String> stringSerde = stingSerdeBuilder.build(appConfigs.createMap(), true);
        final Serde<ActionEvent> actionEventSerde = actionEventSerdeBuilder.build(appConfigs.createMap(), false);
        final Serde<Account> accountSerde = accountSerdeBuilder.build(appConfigs.createMap(), false);

        final StreamsBuilder builder = new StreamsBuilder();

        final StoreBuilder<KeyValueStore<String, Account>> pseudonymStoreBuilder =
                keyValueStoreBuilder(persistentKeyValueStore(appConfigs.storeName("pseudonym")), stringSerde, accountSerde);
        builder.addStateStore(pseudonymStoreBuilder);

        builder.stream(appConfigs.topicName("input"), Consumed.with(stringSerde, actionEventSerde))
            .transform(() -> new PseudonymizeTransformer(TypedStoreRef.fromBuilder(pseudonymStoreBuilder)), pseudonymStoreBuilder.name())
            .to(appConfigs.topicName("output"), Produced.with(stringSerde, actionEventSerde));

        return builder.build();
    }

    public static class PseudonymizeTransformer implements Transformer<String, ActionEvent, KeyValue<String, ActionEvent>> {

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
        public KeyValue<String, ActionEvent> transform(final String accountId, final ActionEvent event) {
            Account pseudonymAccount = pseudonymStore.get(accountId);
            if (pseudonymAccount == null) {
                pseudonymAccount = Account.newBuilder()
                        .setAccountId(randomUUID().toString())
                        .setAccountName(randomAlphabetic(5, 10))
                        .build();
                pseudonymStore.put(accountId, pseudonymAccount);
            }
            final ActionEvent pseudonymizedEvent = ActionEvent.newBuilder(event)
                    .setEventId(randomUUID().toString())
                    .setAccountId(pseudonymAccount.getAccountId())
                    .setAccountName(pseudonymAccount.getAccountName())
                    .build();
            return new KeyValue<>(pseudonymizedEvent.getAccountId(), pseudonymizedEvent);
        }

        @Override
        public void close() {}

    }
}
