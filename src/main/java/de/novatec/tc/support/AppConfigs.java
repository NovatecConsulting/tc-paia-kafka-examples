package de.novatec.tc.support;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.*;

public class AppConfigs {

    private static final Logger LOG = LoggerFactory.getLogger(AppConfigs.class);

    private static final Pattern KEY_VALUE_PATTERN = Pattern.compile("^(?<key>[^=]+)=(?<value>[^=]+)$");
    private static final Pattern TOPIC_CONFIG_PATTERN = Pattern.compile("^(?<name>[^.]+).topic..*$");

    private final Map<String, ?> configs;

    public AppConfigs(final Map<String, ?> configs) {
        this.configs = new HashMap<>(configs);
    }

    public static AppConfigs fromAll(final AppConfigs... appConfigs) {
        final Map<String, Object> combined = new HashMap<>();
        for (final AppConfigs appConfig : appConfigs) {
            combined.putAll(appConfig.configs);
        }
        return new AppConfigs(combined);
    }

    public static AppConfigs fromMap(final Map<String, ?> configs) {
        return new AppConfigs(configs);
    }

    public static AppConfigs fromProperties(final Properties properties) {
        return fromMap(createMap(properties));
    }

    public static AppConfigs fromResource(final String resourceName) {
        return fromURL(requireNonNull(AppConfigs.class.getClassLoader().getResource(resourceName)));
    }

    public static AppConfigs fromURL(final URL url) {
        final Properties allProps = new Properties();
        try(final InputStream input = url.openStream()) {
            allProps.load(input);
        } catch (final IOException e) {
            throw new ConfigException(e.getMessage(), e);
        }
        return fromProperties(allProps);
    }

    public static AppConfigs fromEnv(final String prefix) {
        return fromMap(System.getenv().entrySet().stream()
                .filter(env -> env.getKey().startsWith(prefix))
                .map(env -> Pair.of(env.getKey().replaceFirst(prefix, ""), env.getValue()))
                .map(env -> Pair.of(env.getKey().toLowerCase().replace("_","."), env.getValue()))
                .collect(toMap(Pair::getKey, Pair::getValue)));
    }

    public static AppConfigs fromArgs(final String[] args) {
        final Map<String, Object> configs = new HashMap<>();
        for (String arg : args) {
            final Matcher matcher = KEY_VALUE_PATTERN.matcher(arg);
            if (matcher.matches()) {
                configs.put(matcher.group("key"), matcher.group("value"));
            } else {
                throw new ConfigException(format("Requires parameters in form key=value, but was: %s", arg));
            }
        }
        return fromMap(configs);
    }

    public AppConfigs doLog() {
        LOG.info("{} values:\n{}",
                AppConfigs.class.getSimpleName(),
                configs.entrySet().stream()
                        .map(e -> format("\t%s = %s", e.getKey(), e.getValue()))
                        .collect(joining("\n")));
        return this;
    }

    public Properties createProperties() {
        return createProperties(configs);
    }

    public static Properties createProperties(final Map<String, ?> configs) {
        final Properties properties = new Properties();
        properties.putAll(configs);
        return properties;
    }

    public Map<String, Object> createMap() {
        return new HashMap<>(configs);
    }

    public static Map<String, Object> createMap(final Properties properties) {
        return properties.entrySet().stream()
                .map(e -> Pair.of(String.valueOf(e.getKey()), e.getValue()))
                .collect(toMap(Pair::getKey, Pair::getValue));
    }

    public Object get(String key) {
        return configs.get(key);
    }

    public String storeName(final String configName) {
        return ofNullable(configs.get(format("%s.store.name", configName))).map(this::asStringOrNull).orElse(configName);
    }

    public String topicName(final String configName) {
        return ofNullable(configs.get(format("%s.topic.name", configName))).map(this::asStringOrNull).orElse(configName);
    }

    public Optional<Integer> topicPartitions(final String configName) {
        return ofNullable(configs.get(format("%s.topic.partitions", configName))).map(this::asStringOrNull).map(Integer::valueOf);
    }

    public Optional<Short> topicReplicationFactor(final String configName) {
        return ofNullable(configs.get(format("%s.topic.replication.factor", configName))).map(this::asStringOrNull).map(Short::valueOf);
    }

    public Set<String> topicNames(final String... configNames) {
        return stream(configNames)
                .map(this::topicName)
                .collect(toSet());
    }

    public Set<NewTopic> topics(final String... configNames) {
        return stream(configNames)
                .map(this::topic)
                .collect(toSet());
    }

    public Set<NewTopic> topics() {
        final Set<NewTopic> topics = new HashSet<>();
        for (final String configName : findTopicConfigNames()) {
            topics.add(topic(configName));
        }
        return topics;
    }

    public NewTopic topic(final String configName) {
        final String topicName = topicName(configName);
        final Optional<Integer> partitions = topicPartitions(configName);
        final Optional<Short> replicationFactor = topicReplicationFactor(configName);
        return new NewTopic(topicName, partitions, replicationFactor);
    }

    private Set<String> findTopicConfigNames() {
        final Set<String> configName = new HashSet<>();
        for (final Object key : configs.keySet()) {
            final String config = String.valueOf(key);
            final Matcher matcher = TOPIC_CONFIG_PATTERN.matcher(config);
            if (matcher.matches()) {
                configName.add(matcher.group("name"));
            }
        }
        return configName;
    }

    private String asStringOrNull(final Object value) {
        return value != null ? String.valueOf(value) : null;
    }

}
