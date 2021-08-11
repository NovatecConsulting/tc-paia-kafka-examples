package de.novatec.tc.test.sr;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public record SchemaRegistryInstance(String schemaRegistryUrl, SchemaRegistryClient schemaRegistryClient) {

    public Map<String, Object> clientProperties() {
        return Map.of(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl());
    }

}
