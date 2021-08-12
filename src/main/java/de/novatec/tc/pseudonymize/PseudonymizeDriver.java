package de.novatec.tc.pseudonymize;

import de.novatec.tc.account.v1.Account;
import de.novatec.tc.action.v1.ActionEvent;
import de.novatec.tc.support.AppConfigs;
import de.novatec.tc.support.ScheduledRecordSender;
import de.novatec.tc.support.SerdeBuilder;
import de.novatec.tc.support.TopicSupport;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static java.lang.Math.random;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;

public class PseudonymizeDriver {

    public static void main(final String[] args) {
        final AppConfigs appConfigs = AppConfigs.fromAll(
                AppConfigs.fromResource("pseudonymize.properties"),
                AppConfigs.fromEnv("APP_"),
                AppConfigs.fromArgs(args)).doLog();

        new TopicSupport(appConfigs.createMap())
                .createTopicsIfNotExists(appConfigs.topics("input"), Duration.ofSeconds(10));

        final List<Account> accounts = asList(
                Account.newBuilder().setAccountId("accountA").setAccountName("Anja Abele").build(),
                Account.newBuilder().setAccountId("accountB").setAccountName("Berthold Brecht").build(),
                Account.newBuilder().setAccountId("accountC").setAccountName("Constantin Claußen").build(),
                Account.newBuilder().setAccountId("accountD").setAccountName("Diana Deuss").build());
        final List<String> actions = asList("start", "stop", "accelerate", "retard", "left", "right", "forward", "backward");

        final Serializer<String> stringKeySerializer = SerdeBuilder.fromSerdeSupplier(Serdes.StringSerde::new)
                .build(appConfigs.createMap(), true).serializer();
        final Serializer<ActionEvent> actionEventValueSerializer = SerdeBuilder.<ActionEvent>fromSerdeSupplier(SpecificAvroSerde::new)
                .build(appConfigs.createMap(), false).serializer();

        final ScheduledRecordSender recordSender =
                new ScheduledRecordSender(appConfigs.createMap(), Duration.ofSeconds(10), Duration.ofSeconds(3), 3);
        recordSender.sendAtFixedRate(() -> {
            Account account = accounts.get((int) (random() * accounts.size() - 1));
            ActionEvent event = ActionEvent.newBuilder()
                    .setEventId(randomUUID().toString())
                    .setEventTime(Instant.now())
                    .setAccount(Account.newBuilder(account).build())
                    .setAction(actions.get((int) (random() * actions.size() - 1)))
                    .build();
            return new ProducerRecord<>(appConfigs.topicName("input"), account.getAccountId(), event);
        }, stringKeySerializer, actionEventValueSerializer, Duration.ofSeconds(2));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> recordSender.close(Duration.ofSeconds(10)), "driver-shutdown-hook"));

        recordSender.closeOnNoScheduledSend();
    }

}
