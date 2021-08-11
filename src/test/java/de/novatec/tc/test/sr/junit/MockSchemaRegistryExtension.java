package de.novatec.tc.test.sr.junit;

import de.novatec.tc.test.sr.SchemaRegistryInstance;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

public class MockSchemaRegistryExtension implements AfterTestExecutionCallback, ParameterResolver  {

    private static final String MOCK_URL_SCHEMA = "mock://";

    @Override
    public void afterTestExecution(ExtensionContext context) {
        MockSchemaRegistry.dropScope(getUniqueScope(context));
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return parameterContext.getParameter().getType().equals(SchemaRegistryInstance.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return new SchemaRegistryInstance(
                getUniqueMockUrl(extensionContext),
                MockSchemaRegistry.getClientForScope(getUniqueScope(extensionContext)));
    }

    private String getUniqueMockUrl(ExtensionContext context) {
        return MOCK_URL_SCHEMA + getUniqueScope(context);
    }

    private String getUniqueScope(ExtensionContext context) {
        return Base64.getEncoder().encodeToString(context.getUniqueId().getBytes(UTF_8));
    }

}
