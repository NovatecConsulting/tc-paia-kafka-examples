package de.novatec.tc.pseudonymize;

import de.novatec.tc.account.v1.Account;
import de.novatec.tc.account.v1.ActionEvent;
import de.novatec.tc.support.KafkaSupport;
import de.novatec.tc.support.Settings;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.commons.lang3.RandomStringUtils;
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

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
import static org.apache.kafka.streams.state.Stores.keyValueStoreBuilder;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;

public class PseudonymizeApp {

    public static void main(String[] args) throws IOException, TimeoutException {
        Settings settings = Settings.fromResource("pseudonymize.properties");
        KafkaSupport kafkaSupport = new KafkaSupport(settings.getAll());
        kafkaSupport.createTopicsIfNotExists(settings.topics(), Duration.ofSeconds(5));

        final Serde<String> stringSerde = Serdes.String();
        final Serde<ActionEvent> actionEventSerde = new SpecificAvroSerde<>();
        actionEventSerde.configure(settings.getAsMap(), false);
        final Serde<Account> accountSerde = new SpecificAvroSerde<>();
        accountSerde.configure(settings.getAsMap(), false);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, ActionEvent> actionEvents = builder.stream(settings.topicName("input"),
                Consumed.with(stringSerde, actionEventSerde));

        final String pseudonymStoreName = "pseudonym-store";
        builder.addStateStore(keyValueStoreBuilder(persistentKeyValueStore(pseudonymStoreName), stringSerde, accountSerde));

        KStream<String, ActionEvent> pseudonymizedEvents =
                actionEvents.transform(() -> new PseudonymizeTransformer(pseudonymStoreName), pseudonymStoreName);

        pseudonymizedEvents.to(settings.topicName("output"), Produced.with(stringSerde, actionEventSerde));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, settings.getAll());
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

        private final String pseudonymStoreName;

        private KeyValueStore<String, Account> pseudonymStore;

        public PseudonymizeTransformer(String pseudonymStoreName) {
            this.pseudonymStoreName = pseudonymStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            pseudonymStore = context.getStateStore(pseudonymStoreName);
        }

        @Override
        public KeyValue<String, ActionEvent> transform(String accountId, ActionEvent event) {
            Account pseudonymAccount = pseudonymStore.get(accountId);
            if (pseudonymAccount == null) {
                pseudonymAccount = Account.newBuilder()
                        .setAccountId(UUID.randomUUID().toString())
                        .setAccountName(RandomStringUtils.randomAlphabetic(5, 10))
                        .build();
                pseudonymStore.put(accountId, pseudonymAccount);
            }
            ActionEvent pseudonymizedEvent = ActionEvent.newBuilder(event)
                    .setEventId(UUID.randomUUID().toString())
                    .setAccountId(pseudonymAccount.getAccountId())
                    .setAccountName(pseudonymAccount.getAccountName())
                    .build();
            return new KeyValue<>(pseudonymizedEvent.getAccountId(), pseudonymizedEvent);
        }

        @Override
        public void close() {}

    }
}
