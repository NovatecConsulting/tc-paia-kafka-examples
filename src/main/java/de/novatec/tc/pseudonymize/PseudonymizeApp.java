package de.novatec.tc.pseudonymize;

import de.novatec.tc.account.v1.Account;
import de.novatec.tc.account.v1.ActionEvent;
import de.novatec.tc.support.AppConfigs;
import de.novatec.tc.support.Configs;
import de.novatec.tc.support.TopicsSupport;
import de.novatec.tc.support.TypedStoreRef;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.time.Duration;
import java.util.Map;

import static java.util.UUID.randomUUID;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
import static org.apache.kafka.streams.state.Stores.keyValueStoreBuilder;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;

public class PseudonymizeApp {

    public static void main(String[] args) {
        Map<String, ?> configs = Configs.combined(
                Configs.fromResource("pseudonymize.properties"),
                Configs.fromEnv("APP_"),
                Configs.fromArgs(PseudonymizeApp.class.getSimpleName(), args));

        AppConfigs appConfigs = new AppConfigs(configs);
        new TopicsSupport(appConfigs.asMap())
            .createTopicsIfNotExists(appConfigs.topics(), Duration.ofSeconds(5));

        final Serde<String> stringSerde = Serdes.String();
        final Serde<ActionEvent> actionEventSerde = new SpecificAvroSerde<>();
        actionEventSerde.configure(appConfigs.asMap(), false);
        final Serde<Account> accountSerde = new SpecificAvroSerde<>();
        accountSerde.configure(appConfigs.asMap(), false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ActionEvent> actionEvents = builder.stream(appConfigs.topicName("input"),
                Consumed.with(stringSerde, actionEventSerde));

        StoreBuilder<KeyValueStore<String, Account>> pseudonymStoreBuilder =
                keyValueStoreBuilder(persistentKeyValueStore("pseudonym-store"), stringSerde, accountSerde);
        builder.addStateStore(pseudonymStoreBuilder);

        KStream<String, ActionEvent> pseudonymizedEvents =
                actionEvents.transform(() -> new PseudonymizeTransformer(TypedStoreRef.fromBuilder(pseudonymStoreBuilder)), pseudonymStoreBuilder.name());

        pseudonymizedEvents.to(appConfigs.topicName("output"), Produced.with(stringSerde, actionEventSerde));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, appConfigs.asProperties());
        // https://kafka-tutorials.confluent.io/error-handling/kstreams.html
        streams.setUncaughtExceptionHandler((exception) -> SHUTDOWN_CLIENT);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        try {
            streams.start();
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
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
            // Therefore the actual StateStore must be retrieved from the context.
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
