package de.novatec.tc.pseudonymize;

import de.novatec.tc.account.v1.Account;
import de.novatec.tc.action.v1.ActionEvent;
import de.novatec.tc.support.AppConfigs;
import de.novatec.tc.support.SerdeBuilder;
import de.novatec.tc.support.StreamsApp;
import de.novatec.tc.support.TopicSupport;
import de.novatec.tc.test.client.TestRecordDriver;
import de.novatec.tc.test.client.TopicConsumer;
import de.novatec.tc.test.client.TopicProducer;
import de.novatec.tc.test.sr.SchemaRegistryInstance;
import de.novatec.tc.test.sr.junit.MockSchemaRegistryExtension;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import kafka.server.KafkaConfig$;
import kafka.test.ClusterConfig;
import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ClusterTestDefaults(clusterType = Type.KRAFT)
@ExtendWith(ClusterTestExtensions.class)
@ExtendWith(MockSchemaRegistryExtension.class)
class PseudonymizeAppIntTest {

    @ClusterTest
    void shouldCreateTopicsAndPseudonymizeEvents(ClusterInstance clusterInstance,
                            SchemaRegistryInstance schemaRegistryInstance) {
        final AppConfigs appConfigs = AppConfigs.fromAll(
                AppConfigs.fromResource(PROPERTIES_FILE),
                AppConfigs.fromMap(Map.of(BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
                                          SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryInstance.schemaRegistryUrl(),
                                          AUTO_OFFSET_RESET_CONFIG, "earliest")));

        final SerdeBuilder<String> stringSerdeBuilder = SerdeBuilder.fromSerdeSupplier(Serdes.StringSerde::new);
        final SerdeBuilder<ActionEvent> actionEventSerdeBuilder = SerdeBuilder.fromSerdeSupplier(SpecificAvroSerde::new);

        try (final StreamsApp streamsApp = new PseudonymizeApp().runApp(appConfigs);
             final TestRecordDriver driver = new TestRecordDriver(appConfigs.createMap())) {
            assertThat(new TopicSupport(appConfigs.createMap()).topicsExists(appConfigs.topicNames(), Duration.ofSeconds(1)))
                    .describedAs("At least one of the topics %s does not exists.", appConfigs.topicNames())
                    .isTrue();

            await().timeout(Duration.ofSeconds(5)).until(streamsApp::isInitialized);

            TopicProducer<String, ActionEvent> input = driver
                    .topicProducer(appConfigs.topicName("input"),
                        stringSerdeBuilder.build(appConfigs.createMap(), true).serializer(),
                        actionEventSerdeBuilder.build(appConfigs.createMap(), false).serializer());

            TopicConsumer<String, ActionEvent> output = driver
                    .topicConsumer(appConfigs.topicName("output"),
                            stringSerdeBuilder.build(appConfigs.createMap(), true).deserializer(),
                            actionEventSerdeBuilder.build(appConfigs.createMap(), false).deserializer());

            String inputKey = "accountA";
            ActionEvent inputEvent = ActionEvent.newBuilder()
                    .setEventId(randomUUID().toString())
                    .setEventTime(Instant.now())
                    .setAccount(Account.newBuilder()
                            .setAccountId(inputKey)
                            .setAccountName("Anja Abele").build())
                    .setAction("start")
                    .build();
            input.pipeInput(inputKey, inputEvent);

            ConsumerRecord<String, ActionEvent> receivedEvent = output.readKeyValue();
            assertThat(receivedEvent.key()).isNotBlank();
            assertThat(receivedEvent.key()).isNotEqualTo(inputKey);
            assertThat(receivedEvent.value()).isNotNull();
            assertThat(receivedEvent.value().getAccount().getAccountId()).isNotBlank();
            assertThat(receivedEvent.value().getAccount().getAccountId()).isNotEqualTo(inputEvent.getAccount().getAccountId());
        }
    }

    @ClusterTest
    void shouldRecoverStateAndRememberPseudonymizeMappingWithNewStateStoreDir(ClusterInstance clusterInstance,
                                                                               SchemaRegistryInstance schemaRegistryInstance,
                                                                               @TempDir Path stateStoreDir1, @TempDir Path stateStoreDir2) {
        final AppConfigs appConfigs = AppConfigs.fromAll(
                AppConfigs.fromResource(PROPERTIES_FILE),
                AppConfigs.fromMap(Map.of(BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
                        SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryInstance.schemaRegistryUrl(),
                        AUTO_OFFSET_RESET_CONFIG, "earliest")));

        new TopicSupport(appConfigs.createMap()).createTopicsIfNotExists(appConfigs.topics(), Duration.ofSeconds(5));

        final SerdeBuilder<String> stringSerdeBuilder = SerdeBuilder.fromSerdeSupplier(Serdes.StringSerde::new);
        final SerdeBuilder<ActionEvent> actionEventSerdeBuilder = SerdeBuilder.fromSerdeSupplier(SpecificAvroSerde::new);

        try (final TestRecordDriver driver = new TestRecordDriver(appConfigs.createMap())) {
            TopicProducer<String, ActionEvent> input = driver
                    .topicProducer(appConfigs.topicName("input"),
                            stringSerdeBuilder.build(appConfigs.createMap(), true).serializer(),
                            actionEventSerdeBuilder.build(appConfigs.createMap(), false).serializer());

            TopicConsumer<String, ActionEvent> output = driver
                    .topicConsumer(appConfigs.topicName("output"),
                            stringSerdeBuilder.build(appConfigs.createMap(), true).deserializer(),
                            actionEventSerdeBuilder.build(appConfigs.createMap(), false).deserializer());

            String inputKey = "accountA";
            ActionEvent inputEvent = ActionEvent.newBuilder()
                    .setEventId(randomUUID().toString())
                    .setEventTime(Instant.now())
                    .setAccount(Account.newBuilder()
                            .setAccountId(inputKey)
                            .setAccountName("Anja Abele").build())
                    .setAction("start")
                    .build();

            // start application, send event and close application
            ConsumerRecord<String, ActionEvent> receivedEvent1;
            try (final StreamsApp streamsApp = new PseudonymizeApp().runApp(appConfigs.createAppConfigsWith(newConfigs ->
                    newConfigs.put(STATE_DIR_CONFIG, stateStoreDir1.toString())))) {
                await().timeout(Duration.ofSeconds(5)).until(streamsApp::isInitialized);
                input.pipeInput(inputKey, inputEvent);
                receivedEvent1 = output.readKeyValue();
            }

            // start application with different state store directory, send event and close application
            ConsumerRecord<String, ActionEvent> receivedEvent2;
            try (final StreamsApp streamsApp = new PseudonymizeApp().runApp(appConfigs.createAppConfigsWith(newConfigs ->
                    newConfigs.put(STATE_DIR_CONFIG, stateStoreDir2.toString())))) {
                await().timeout(Duration.ofSeconds(5)).until(streamsApp::isInitialized);
                input.pipeInput(inputKey, inputEvent);
                receivedEvent2 = output.readKeyValue();
            }

            assertThat(receivedEvent1.key()).isEqualTo(receivedEvent2.key());
            // This is a deliberately simple implementation
            // that always uses the same values per account for all other person-related fields.
            assertThat(receivedEvent1.value().getAccount().getAccountName()).isEqualTo(receivedEvent2.value().getAccount().getAccountName());
            // The event id is unique for each event,
            // so a new id is simply always generated here to prevent a reference to the source event.
            assertThat(receivedEvent1.value().getEventId()).isNotEqualTo(receivedEvent2.value().getEventId());
        }
    }

    static final String PROPERTIES_FILE = "pseudonymize.properties";

    @BeforeEach
    void initClusterConfigs(ClusterConfig clusterConfig) {
        clusterConfig.serverProperties().put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), false);
        clusterConfig.serverProperties().put(KafkaConfig$.MODULE$.GroupInitialRebalanceDelayMsProp(), 0);
        clusterConfig.serverProperties().put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), 1);
        clusterConfig.serverProperties().put(KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), 1);
    }

}