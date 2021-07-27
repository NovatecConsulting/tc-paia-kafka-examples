package de.novatec.tc.pseudonymize;

import de.novatec.tc.account.v1.Account;
import de.novatec.tc.account.v1.ActionEvent;
import de.novatec.tc.support.KafkaSupport;
import de.novatec.tc.support.Settings;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Arrays.asList;

public class PseudonymizeDriver {

    public static void main(String[] args) throws IOException, TimeoutException, ExecutionException, InterruptedException {
        Settings settings = Settings.fromResource("pseudonymize.properties")
                .withProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName())
                .withProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
        KafkaSupport kafkaSupport = new KafkaSupport(settings.getAll());
        kafkaSupport.createTopicsIfNotExists(settings.topics("input"), Duration.ofSeconds(5));

        List<Account> accounts = new ArrayList<>();
        accounts.add(Account.newBuilder().setAccountId("accountA").setAccountName("Anja Abele").build());
        accounts.add(Account.newBuilder().setAccountId("accountB").setAccountName("Berthold Brecht").build());
        accounts.add(Account.newBuilder().setAccountId("accountC").setAccountName("Constantin Claußen").build());
        accounts.add(Account.newBuilder().setAccountId("accountD").setAccountName("Diana Deuss").build());
        List<String> actions = asList("start", "stop", "accelerate", "retard", "left", "right", "forward", "backward");

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        try(KafkaProducer<String, ActionEvent> producer = new KafkaProducer<>(settings.getAll())) {
            while (running.get()) {
                Account account = accounts.get((int) (Math.random() * accounts.size() - 1));
                ActionEvent event = ActionEvent.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setEventTime(Instant.now())
                        .setAccountId(account.getAccountId())
                        .setAccountName(account.getAccountName())
                        .setAction(actions.get((int) (Math.random() * actions.size() - 1)))
                        .build();
                producer.send(new ProducerRecord<>(settings.topicName("input"), account.getAccountId(), event)).get();
                Thread.sleep(2000);
            }
        }
    }

}
