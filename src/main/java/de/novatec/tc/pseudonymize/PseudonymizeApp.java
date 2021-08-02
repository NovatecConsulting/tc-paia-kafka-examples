package de.novatec.tc.pseudonymize;

import de.novatec.tc.account.v1.Account;
import de.novatec.tc.account.v1.ActionEvent;
import de.novatec.tc.support.AppConfigs;
import de.novatec.tc.support.SerdeBuilder;
import de.novatec.tc.support.TopicSupport;
import de.novatec.tc.support.TypedStoreRef;
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
import java.util.concurrent.CountDownLatch;

import static de.novatec.tc.support.FileSupport.deleteHook;
import static de.novatec.tc.support.FileSupport.tempDirectory;
import static java.util.UUID.randomUUID;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
import static org.apache.kafka.streams.state.Stores.keyValueStoreBuilder;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;

public class PseudonymizeApp {

    public static void main(final String[] args) {
        final AppConfigs appConfigs = AppConfigs.fromAll(
                AppConfigs.fromResource("pseudonymize.properties"),
                AppConfigs.fromEnv("APP_"),
                AppConfigs.fromArgs(args)).doLog();

        new PseudonymizeApp().runApp(appConfigs);
    }

    public void runApp(final AppConfigs appConfigs) {
        new TopicSupport(appConfigs.createMap())
                .createTopicsIfNotExists(appConfigs.topics(), Duration.ofSeconds(5));

        final CountDownLatch cleanUpMonitor = new CountDownLatch(1);
        final AppConfigs actualAppConfigs = appConfigs.createAppConfigsWith(newConfigs ->
            newConfigs.computeIfAbsent(STATE_DIR_CONFIG,
                    key -> deleteHook(tempDirectory("kafka-streams"), cleanUpMonitor::await, Duration.ofSeconds(10)).getPath())
        );

        final KafkaStreams streams = new KafkaStreams(buildTopology(actualAppConfigs), actualAppConfigs.createProperties());
        streams.setUncaughtExceptionHandler((exception) -> SHUTDOWN_CLIENT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            cleanUpMonitor.countDown();
        }, "streams-shutdown-hook"));

        streams.start();
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
