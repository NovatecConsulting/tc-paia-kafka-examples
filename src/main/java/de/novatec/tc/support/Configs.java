package de.novatec.tc.support;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public final class Configs {

    private Configs() {}

    @SafeVarargs
    public static Map<String, Object> combined(final Map<String, ?>... configs) {
        final Map<String, Object> combined = new HashMap<>();
        for (final Map<String, ?> config : configs) {
            combined.putAll(config);
        }
        return combined;
    }

    public static Map<String, Object> fromResource(final String resourceName) {
        return fromURL(requireNonNull(AppConfigs.class.getClassLoader().getResource(resourceName)));
    }

    public static Map<String, Object> fromURL(final URL url) {
        final Properties allProps = new Properties();
        try(final InputStream input = url.openStream()) {
            allProps.load(input);
        } catch (final IOException e) {
            throw new ConfigException(e.getMessage(), e);
        }
        return asMap(allProps);
    }

    public static Map<String, Object> fromEnv(final String prefix) {
        return System.getenv().entrySet().stream()
                .filter(env -> env.getKey().startsWith(prefix))
                .map(env -> Pair.of(env.getKey().toLowerCase().replace("_","."), env.getValue()))
                .collect(toMap(Pair::getKey, Pair::getValue));
    }

    public static Map<String, Object> fromArgs(final String name, final String[] args) {
        return new CliConfigProvider(name, args).get();
    }

    private static Map<String, Object> asMap(final Properties properties) {
        return properties.entrySet().stream()
                .map(e -> Pair.of(String.valueOf(e.getKey()), e.getValue()))
                .collect(toMap(Pair::getKey, Pair::getValue));
    }
}
