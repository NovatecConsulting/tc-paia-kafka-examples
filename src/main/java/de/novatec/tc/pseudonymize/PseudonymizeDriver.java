package de.novatec.tc.pseudonymize;

import de.novatec.tc.account.v1.Account;
import de.novatec.tc.account.v1.ActionEvent;
import de.novatec.tc.support.AppConfigs;
import de.novatec.tc.support.Configs;
import de.novatec.tc.support.ScheduledRecordSender;
import de.novatec.tc.support.TopicsSupport;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static java.lang.Math.random;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;

public class PseudonymizeDriver {

    public static void main(String[] args) {
        Map<String, Object> configs = Configs.combined(
                Configs.fromResource("pseudonymize.properties"),
                Configs.fromEnv("APP_"),
                Configs.fromArgs(PseudonymizeApp.class.getSimpleName(), args));

        AppConfigs appConfigs = new AppConfigs(configs);
        new TopicsSupport(appConfigs.asMap())
                .createTopicsIfNotExists(appConfigs.topics("input"), Duration.ofSeconds(10));

        List<Account> accounts = asList(
                Account.newBuilder().setAccountId("accountA").setAccountName("Anja Abele").build(),
                Account.newBuilder().setAccountId("accountB").setAccountName("Berthold Brecht").build(),
                Account.newBuilder().setAccountId("accountC").setAccountName("Constantin Claußen").build(),
                Account.newBuilder().setAccountId("accountD").setAccountName("Diana Deuss").build());
        List<String> actions = asList("start", "stop", "accelerate", "retard", "left", "right", "forward", "backward");

        ScheduledRecordSender recordSender = new ScheduledRecordSender(configs, Duration.ofSeconds(10), Duration.ofSeconds(5), 3);
        recordSender.sendAtFixedRate(() -> {
            Account account = accounts.get((int) (random() * accounts.size() - 1));
            ActionEvent event = ActionEvent.newBuilder()
                    .setEventId(randomUUID().toString())
                    .setEventTime(Instant.now())
                    .setAccountId(account.getAccountId())
                    .setAccountName(account.getAccountName())
                    .setAction(actions.get((int) (random() * actions.size() - 1)))
                    .build();
            return new ProducerRecord<>(appConfigs.topicName("input"), account.getAccountId(), event);
        }, StringSerializer.class, SpecificAvroSerializer.class, Duration.ofSeconds(2));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> recordSender.close(Duration.ofSeconds(10))));
        recordSender.closeOnNoScheduledSend();
    }

}
