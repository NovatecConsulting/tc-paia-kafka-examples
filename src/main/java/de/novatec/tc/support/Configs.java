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

public interface Configs {

    @SafeVarargs
    static Map<String, Object> combined(Map<String, ?>... configs) {
        Map<String, Object> combined = new HashMap<>();
        for (Map<String, ?> config : configs) {
            combined.putAll(config);
        }
        return combined;
    }

    static Map<String, Object> fromResource(String resourceName) {
        return fromURL(requireNonNull(AppConfigs.class.getClassLoader().getResource(resourceName)));
    }

    static Map<String, Object> fromURL(URL url) {
        Properties allProps = new Properties();
        try(InputStream input = url.openStream()) {
            allProps.load(input);
        } catch (IOException e) {
            throw new ConfigException(e.getMessage(), e);
        }
        return asMap(allProps);
    }

    static Map<String, Object> fromEnv(String prefix) {
        return System.getenv().entrySet().stream()
                .filter(env -> env.getKey().startsWith(prefix))
                .map(env -> Pair.of(env.getKey().toLowerCase().replace("_","."), env.getValue()))
                .collect(toMap(Pair::getKey, Pair::getValue));
    }

    static Map<String, Object> fromArgs(String name, String[] args) {
        return new CliConfigProvider(name, args).get();
    }

    private static Map<String, Object> asMap(Properties properties) {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            map.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return map;
    }
}
