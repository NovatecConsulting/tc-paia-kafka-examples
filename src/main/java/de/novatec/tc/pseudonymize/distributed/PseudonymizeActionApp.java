package de.novatec.tc.pseudonymize.distributed;

import de.novatec.tc.account.v1.Account;
import de.novatec.tc.account.v1.CreatePseudonymAccountCommand;
import de.novatec.tc.account.v1.PseudonymAccountEvent;
import de.novatec.tc.action.v1.ActionEvent;
import de.novatec.tc.support.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static de.novatec.tc.support.FileSupport.deleteCloseable;
import static de.novatec.tc.support.FileSupport.tempDirectory;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.apache.kafka.streams.state.Stores.keyValueStoreBuilder;
import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;

public class PseudonymizeActionApp {

    public static void main(final String[] args) {
        final AppConfigs appConfigs = AppConfigs.fromAll(
                AppConfigs.fromResource("pseudonymize-distributed-action.properties"),
                AppConfigs.fromEnv("APP_"),
                AppConfigs.fromArgs(args)).doLog();

        new PseudonymizeActionApp()
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
        final SerdeBuilder<CreatePseudonymAccountCommand> commandSerdeBuilder = SerdeBuilder.fromSerdeSupplier(SpecificAvroSerde::new);
        final SerdeBuilder<PseudonymAccountEvent> pseudonymAccountSerdeBuilder = SerdeBuilder.fromSerdeSupplier(SpecificAvroSerde::new);
        final SerdeBuilder<Account> accountSerdeBuilder = SerdeBuilder.fromSerdeSupplier(SpecificAvroSerde::new);
        return buildTopology(appConfigs, stringSerdeBuilder, actionEventSerdeBuilder, commandSerdeBuilder, pseudonymAccountSerdeBuilder, accountSerdeBuilder);
    }

    public Topology buildTopology(final AppConfigs appConfigs,
                                  final SerdeBuilder<String> stingSerdeBuilder,
                                  final SerdeBuilder<ActionEvent> actionEventSerdeBuilder,
                                  final SerdeBuilder<CreatePseudonymAccountCommand> commandSerdeBuilder,
                                  final SerdeBuilder<PseudonymAccountEvent> pseudonymAccountSerdeBuilder,
                                  final SerdeBuilder<Account> accountSerdeBuilder) {
        final Serde<String> stringSerde = stingSerdeBuilder.build(appConfigs.createMap(), true);
        final Serde<ActionEvent> actionEventSerde = actionEventSerdeBuilder.build(appConfigs.createMap(), false);
        final Serde<CreatePseudonymAccountCommand> commandSerde = commandSerdeBuilder.build(appConfigs.createMap(), false);
        final Serde<PseudonymAccountEvent> pseudonymAccountSerde  = pseudonymAccountSerdeBuilder.build(appConfigs.createMap(), false);
        final Serde<Account> accountSerde = accountSerdeBuilder.build(appConfigs.createMap(), false);

        final StreamsBuilder builder = new StreamsBuilder();

        final StoreBuilder<KeyValueStore<String, Account>> pseudonymStoreBuilder =
                keyValueStoreBuilder(persistentKeyValueStore(appConfigs.storeName("pseudonymaccount")), stringSerde, accountSerde);
        builder.addStateStore(pseudonymStoreBuilder);

        final StoreBuilder<KeyValueStore<String, ActionEvent>> actionEventStoreBuilder =
                keyValueStoreBuilder(persistentKeyValueStore(appConfigs.storeName("actionevent")), stringSerde, actionEventSerde);
        builder.addStateStore(actionEventStoreBuilder);

        builder.stream(appConfigs.topicName("action.input"), Consumed.with(stringSerde, actionEventSerde))
                .transformValues(() -> new LookupPseudonymAccount(TypedStoreRef.fromBuilder(pseudonymStoreBuilder)), pseudonymStoreBuilder.name())
                .split()
                    .branch(
                            (accountId, actionEventAndPseudonymAccount) -> actionEventAndPseudonymAccount.getValue() == null,
                            Branched.withConsumer(noPseudonymAccountStream -> {
                                noPseudonymAccountStream
                                    .mapValues((accountId, actionEventAndPseudonymAccount) ->
                                            CreatePseudonymAccountCommand.newBuilder()
                                                    .setEventId(randomUUID().toString())
                                                    .setEventTime(Instant.now())
                                                    .setAccount(actionEventAndPseudonymAccount.getKey().getAccount()).build())
                                    .to(appConfigs.topicName("command.output"), Produced.with(stringSerde, commandSerde));
                                noPseudonymAccountStream
                                        .mapValues((accountId, actionEventAndPseudonymAccount) -> actionEventAndPseudonymAccount.getKey())
                                        .transformValues(() -> new AddActionEventToWaitingRoom(TypedStoreRef.fromBuilder(actionEventStoreBuilder)), actionEventStoreBuilder.name());
                            })
                    )
                    .defaultBranch(
                            Branched.withConsumer(pseudonymAccountStream -> pseudonymAccountStream
                                    .mapValues((accountId, actionEventAndPseudonymAccount) ->
                                            ActionEvent.newBuilder(actionEventAndPseudonymAccount.getKey())
                                                    .setEventId(randomUUID().toString())
                                                    .setAccount(Account.newBuilder(actionEventAndPseudonymAccount.getValue()).build())
                                                    .build())
                                    .to(appConfigs.topicName("action.output"), Produced.with(stringSerde, actionEventSerde)))
                    );

        final KStream<String, PseudonymAccountEvent> pseudonymAccountStream =
                builder.stream(appConfigs.topicName("pseudonymaccount.input"), Consumed.with(stringSerde, pseudonymAccountSerde));
        pseudonymAccountStream
                .flatTransform(() -> new PseudonymizeActionEventsInWaitingRoom(TypedStoreRef.fromBuilder(actionEventStoreBuilder)), actionEventStoreBuilder.name())
                .to(appConfigs.topicName("action.output"), Produced.with(stringSerde, actionEventSerde));
        pseudonymAccountStream
                .transformValues(() -> new RemoveActionEventsFromWaitingRoom(TypedStoreRef.fromBuilder(actionEventStoreBuilder)), actionEventStoreBuilder.name())
                .transformValues(() -> new AddNewPseudonymAccount(TypedStoreRef.fromBuilder(pseudonymStoreBuilder)), pseudonymStoreBuilder.name());

        return builder.build();
    }

    public static class LookupPseudonymAccount implements ValueTransformerWithKey<String, ActionEvent, Pair<ActionEvent, Account>> {

        private final TypedStoreRef<KeyValueStore<String, Account>> pseudonymStoreRef;
        private ReadOnlyKeyValueStore<String, Account> pseudonymStore;

        public LookupPseudonymAccount(final TypedStoreRef<KeyValueStore<String, Account>> pseudonymStoreRef) {
            this.pseudonymStoreRef = pseudonymStoreRef;
        }

        @Override
        public void init(final ProcessorContext context) {
            pseudonymStore = pseudonymStoreRef.getStateStore(context);
        }

        @Override
        public Pair<ActionEvent, Account> transform(final String accountId, final ActionEvent event) {
            final Account pseudonymAccount = pseudonymStore.get(accountId);
            return ImmutablePair.of(event, pseudonymAccount);
        }

        @Override
        public void close() {}

    }

    public static class AddActionEventToWaitingRoom implements ValueTransformerWithKey<String, ActionEvent, ActionEvent> {

        private final TypedStoreRef<KeyValueStore<String, ActionEvent>> actionEventStoreRef;
        private KeyValueStore<String, ActionEvent> actionEventStore;

        public AddActionEventToWaitingRoom(final TypedStoreRef<KeyValueStore<String, ActionEvent>> actionEventStoreRef) {
            this.actionEventStoreRef = actionEventStoreRef;
        }

        @Override
        public void init(final ProcessorContext context) {
            actionEventStore = actionEventStoreRef.getStateStore(context);
        }

        @Override
        public ActionEvent transform(final String accountId, final ActionEvent event) {
            actionEventStore.put(accountId + "#" + event.getEventId(), event);
            return event;
        }

        @Override
        public void close() {}

    }

    public static class PseudonymizeActionEventsInWaitingRoom implements Transformer<String, PseudonymAccountEvent, Iterable<KeyValue<String, ActionEvent>>> {

        private final TypedStoreRef<KeyValueStore<String, ActionEvent>> actionEventStoreRef;
        private ReadOnlyKeyValueStore<String, ActionEvent> actionEventStore;

        public PseudonymizeActionEventsInWaitingRoom(final TypedStoreRef<KeyValueStore<String, ActionEvent>> actionEventStoreRef) {
            this.actionEventStoreRef = actionEventStoreRef;
        }

        @Override
        public void init(final ProcessorContext context) {
            actionEventStore = actionEventStoreRef.getStateStore(context);
        }

        @Override
        public Iterable<KeyValue<String, ActionEvent>> transform(final String accountId, final PseudonymAccountEvent pseudonymAccount) {
            final List<KeyValue<String, ActionEvent>> pseudonymizedEvents = new ArrayList<>();
            try (KeyValueIterator<String, ActionEvent> it = actionEventStore.prefixScan(accountId + "#", new StringSerializer())) {
                while (it.hasNext()) {
                    final KeyValue<String, ActionEvent> original = it.next();
                    final String pseudonymizedAccountId = pseudonymAccount.getAccount().getAccountId();
                    final ActionEvent pseudonymizedActionEvent = ActionEvent.newBuilder(original.value)
                            .setEventId(randomUUID().toString())
                            .setAccountBuilder(Account.newBuilder(pseudonymAccount.getAccount())).build();
                    pseudonymizedEvents.add(new KeyValue<>(pseudonymizedAccountId, pseudonymizedActionEvent));
                }
            }

            return pseudonymizedEvents;
        }

        @Override
        public void close() {}

    }

    public static class RemoveActionEventsFromWaitingRoom implements ValueTransformerWithKey<String, PseudonymAccountEvent, PseudonymAccountEvent> {

        private final TypedStoreRef<KeyValueStore<String, ActionEvent>> actionEventStoreRef;
        private KeyValueStore<String, ActionEvent> actionEventStore;

        public RemoveActionEventsFromWaitingRoom(final TypedStoreRef<KeyValueStore<String, ActionEvent>> actionEventStoreRef) {
            this.actionEventStoreRef = actionEventStoreRef;
        }

        @Override
        public void init(final ProcessorContext context) {
            actionEventStore = actionEventStoreRef.getStateStore(context);
        }

        @Override
        public PseudonymAccountEvent transform(final String accountId, final PseudonymAccountEvent pseudonymAccount) {
            final Set<String> keysToDelete = new HashSet<>();
            try (KeyValueIterator<String, ActionEvent> it = actionEventStore.prefixScan(accountId + "#", new StringSerializer())) {
                while (it.hasNext()) {
                    final KeyValue<String, ActionEvent> original = it.next();
                    keysToDelete.add(original.key);
                }
            }
            keysToDelete.forEach(actionEventStore::delete);

            return pseudonymAccount;
        }

        @Override
        public void close() {}

    }

    public static class AddNewPseudonymAccount implements ValueTransformerWithKey<String, PseudonymAccountEvent, PseudonymAccountEvent> {

        private final TypedStoreRef<KeyValueStore<String, Account>> pseudonymStoreRef;
        private KeyValueStore<String, Account> pseudonymStore;

        public AddNewPseudonymAccount(final TypedStoreRef<KeyValueStore<String, Account>> pseudonymStoreRef) {
            this.pseudonymStoreRef = pseudonymStoreRef;
        }

        @Override
        public void init(final ProcessorContext context) {
            pseudonymStore = pseudonymStoreRef.getStateStore(context);
        }

        @Override
        public PseudonymAccountEvent transform(final String accountId, final PseudonymAccountEvent event) {
            pseudonymStore.put(accountId, event.getAccount());
            return event;
        }

        @Override
        public void close() {}

    }
}
