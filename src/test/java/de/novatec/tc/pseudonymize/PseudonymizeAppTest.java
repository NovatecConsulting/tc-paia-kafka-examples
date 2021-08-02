package de.novatec.tc.pseudonymize;

import de.novatec.tc.account.v1.Account;
import de.novatec.tc.account.v1.ActionEvent;
import de.novatec.tc.support.AppConfigs;
import de.novatec.tc.support.SerdeBuilder;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.function.Consumer;

import static de.novatec.tc.support.FileSupport.deleteQuietly;
import static de.novatec.tc.support.FileSupport.tempDirectory;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PseudonymizeAppTest {

    @Test
    void shouldPseudonymizePersonalData() {
        runWithTopologyTestDriver(scope -> {
            // Produce some input data to the input topic.
            String inputKey = "accountA";
            ActionEvent inputEvent = ActionEvent.newBuilder()
                    .setEventId(randomUUID().toString())
                    .setEventTime(Instant.now())
                    .setAccountId(inputKey)
                    .setAccountName("Anja Abele")
                    .setAction("start")
                    .build();
            scope.input.pipeInput(inputKey, inputEvent);

            // Verify the application's output data.
            KeyValue<String, ActionEvent> receivedEvent = scope.output.readKeyValue();
            assertThat(receivedEvent.key).isNotBlank();
            assertThat(receivedEvent.key).isNotEqualTo(inputKey);
            assertThat(receivedEvent.value).isNotNull();
            assertThat(receivedEvent.value.getAccountId()).isNotBlank();
            assertThat(receivedEvent.value.getAccountId()).isNotEqualTo(inputEvent.getAccountId());
            assertThat(receivedEvent.value.getAccountName()).isNotBlank();
            assertThat(receivedEvent.value.getAccountName()).isNotEqualTo(inputEvent.getAccountName());
            assertThat(receivedEvent.value.getEventId()).isNotBlank();
            assertThat(receivedEvent.value.getEventId()).isNotEqualTo(inputEvent.getEventId());
            assertThat(receivedEvent.value.getEventTime()).isEqualTo(inputEvent.getEventTime());
            assertThat(receivedEvent.value.getAction()).isEqualTo(inputEvent.getAction());
            assertThat(receivedEvent.key).isEqualTo(receivedEvent.value.getAccountId());
            assertTrue(scope.output.isEmpty());
        });
    }

    @Test
    void shouldPseudonymizeWithSameValuesForSameKey() {
        runWithTopologyTestDriver(scope -> {
            // Produce some input data to the input topic.
            String inputKeyA = "accountA";
            ActionEvent inputEventA1 = ActionEvent.newBuilder()
                    .setEventId(randomUUID().toString())
                    .setEventTime(Instant.now())
                    .setAccountId(inputKeyA)
                    .setAccountName("Anja Abele")
                    .setAction("start")
                    .build();
            scope.input.pipeInput(inputKeyA, inputEventA1);
            ActionEvent inputEventA2 = ActionEvent.newBuilder()
                    .setEventId(randomUUID().toString())
                    .setEventTime(Instant.now())
                    .setAccountId(inputKeyA)
                    .setAccountName("Diana Deuss")
                    .setAction("accelerate")
                    .build();
            scope.input.pipeInput(inputKeyA, inputEventA2);

            // Verify the application's output data.
            KeyValue<String, ActionEvent> receivedEvent1 = scope.output.readKeyValue();
            KeyValue<String, ActionEvent> receivedEvent2 = scope.output.readKeyValue();
            assertThat(receivedEvent1.value.getAction()).isEqualTo(inputEventA1.getAction());
            assertThat(receivedEvent2.value.getAction()).isEqualTo(inputEventA2.getAction());
            assertThat(receivedEvent1.key).isEqualTo(receivedEvent2.key);
            // This is a deliberately simple implementation
            // that always uses the same values per account for all other person-related fields.
            assertThat(receivedEvent1.value.getAccountName()).isEqualTo(receivedEvent2.value.getAccountName());
            // The event id is unique for each event,
            // so a new id is simply always generated here to prevent a reference to the source event.
            assertThat(receivedEvent1.value.getEventId()).isNotEqualTo(receivedEvent2.value.getEventId());
            assertTrue(scope.output.isEmpty());
        });
    }

    @Test
    void shouldPseudonymizeWithDifferentValuesForDifferentKey() {
        runWithTopologyTestDriver(scope -> {
            // Produce some input data to the input topic.
            String inputKeyA = "accountA";
            ActionEvent inputEventA1 = ActionEvent.newBuilder()
                    .setEventId(randomUUID().toString())
                    .setEventTime(Instant.now())
                    .setAccountId(inputKeyA)
                    .setAccountName("Anja Abele")
                    .setAction("start")
                    .build();
            scope.input.pipeInput(inputKeyA, inputEventA1);
            String inputKeyB = "accountB";
            ActionEvent inputEventB1 = ActionEvent.newBuilder()
                    .setEventId(randomUUID().toString())
                    .setEventTime(Instant.now())
                    .setAccountId(inputKeyB)
                    .setAccountName("Anja Abele")
                    .setAction("accelerate")
                    .build();
            scope.input.pipeInput(inputKeyB, inputEventB1);

            // Verify the application's output data.
            KeyValue<String, ActionEvent> receivedEvent1 = scope.output.readKeyValue();
            KeyValue<String, ActionEvent> receivedEvent2 = scope.output.readKeyValue();
            assertThat(receivedEvent1.value.getAction()).isEqualTo(inputEventA1.getAction());
            assertThat(receivedEvent2.value.getAction()).isEqualTo(inputEventB1.getAction());
            assertThat(receivedEvent1.key).isNotEqualTo(receivedEvent2.key);
            assertThat(receivedEvent1.value.getAccountName()).isNotEqualTo(receivedEvent2.value.getAccountName());
            assertThat(receivedEvent1.value.getEventId()).isNotEqualTo(receivedEvent2.value.getEventId());
            assertTrue(scope.output.isEmpty());
        });
    }

    static final String PROPERTIES_FILE = "pseudonymize.properties";
    static final String SCHEMA_REGISTRY_SCOPE = PseudonymizeAppTest.class.getName();
    static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

    void runWithTopologyTestDriver(Consumer<TestScope> testAction) {
        final AppConfigs appConfigs = AppConfigs.fromAll(
                AppConfigs.fromResource(PROPERTIES_FILE),
                AppConfigs.fromMap(Map.of(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL)),
                AppConfigs.fromMap(Map.of(STATE_DIR_CONFIG, tempDirectory("kafka-streams").getPath())));

        final SerdeBuilder<String> stringSerdeBuilder = SerdeBuilder.fromSerdeSupplier(Serdes.StringSerde::new);
        final SerdeBuilder<ActionEvent> actionEventSerdeBuilder = SerdeBuilder.fromSerdeSupplier(SpecificAvroSerde::new);

        final Topology topology = new PseudonymizeApp().buildTopology(appConfigs);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, appConfigs.createProperties())) {

            // Setup input and output topics.
            final TestInputTopic<String, ActionEvent> input = driver
                    .createInputTopic(appConfigs.topicName("input"),
                            stringSerdeBuilder.build(appConfigs.createMap(), true).serializer(),
                            actionEventSerdeBuilder.build(appConfigs.createMap(), false).serializer());
            final TestOutputTopic<String, ActionEvent> output = driver
                    .createOutputTopic(appConfigs.topicName("output"),
                            stringSerdeBuilder.build(appConfigs.createMap(), true).deserializer(),
                            actionEventSerdeBuilder.build(appConfigs.createMap(), false).deserializer());

            // Retrieve state stores
            final KeyValueStore<String, Account> pseudonymStore = driver.getKeyValueStore(appConfigs.storeName("pseudonym"));

            // Run actual test action
            testAction.accept(new TestScope(input, output, pseudonymStore));

        } finally {
            // Clean up
            MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
            deleteQuietly(Path.of(String.valueOf(appConfigs.get(STATE_DIR_CONFIG))).toFile());
        }
    }

    static record TestScope(TestInputTopic<String, ActionEvent> input,
                            TestOutputTopic<String, ActionEvent> output,
                            KeyValueStore<String, Account> pseudonymStore) {}
}