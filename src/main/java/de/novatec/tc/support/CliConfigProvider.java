package de.novatec.tc.support;

import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;
import org.apache.kafka.common.config.ConfigException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class CliConfigProvider {

    private static final String DEFAULT_NAME = "cli";

    private final String[] args;

    private final ArgumentParser parser;

    public CliConfigProvider(final String[] args) {
        this(DEFAULT_NAME, args);
    }

    public CliConfigProvider(final String name, final String[] args) {
        this(name, null, args);
    }

    public CliConfigProvider(final String name, final String description, final String[] args) {
        this(parserBuilder(name), description, args);
    }

    CliConfigProvider(final ArgumentParserBuilder parserBuilder, final String description, final String[] args) {
        this(initParser(parserBuilder, description), args);
    }

    CliConfigProvider(final ArgumentParser parser, final String[] args) {
        this.parser = parser;
        this.args = args;
    }

    public Map<String, Object> get() {
        final Map<String, Object> attrs = new HashMap<>();
        try {
            parser.parseArgs(args, attrs);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            throw new ConfigException(e.getMessage());
        }
        attrs.remove("property");
        attrs.remove("properties_file");
        return attrs;
    }

    private static ArgumentParserBuilder parserBuilder(final String name) {
        return ArgumentParsers
                .newFor(name)
                .addHelp(true);
    }

    private static ArgumentParser initParser(final ArgumentParserBuilder parserBuilder, final String description) {
        final ArgumentParser parser = parserBuilder.build();
        Optional.ofNullable(description).ifPresent(parser::description);
        parser.addArgument("--property")
                .action(new FlapMapArgumentAction())
                .required(false)
                .type(new KeyValueArgumentType())
                .metavar("Key=Value")
                .help("Config property. Properties given by this way have precedence over properties given by properties file.");
        parser.addArgument("--properties-file")
                .action(new FlapMapArgumentAction())
                .required(false)
                .type(new PropertiesFileArgumentType())
                .metavar("Properties File")
                .help("Config properties file.");
        return parser;
    }

    private static class KeyValueArgumentType implements ArgumentType<Map<String, String>> {

        private final Pattern PATTERN = Pattern.compile("([^=]+)=([^=]+)");

        @Override
        public Map<String, String> convert(final ArgumentParser parser, final Argument arg, final String value) throws ArgumentParserException {
            final Matcher matcher = PATTERN.matcher(value);
            if (matcher.matches()) {
                return Map.of(matcher.group(1), matcher.group(2));
            }
            throw new ArgumentParserException(format("Invalid property format provided. Expected format witch matches pattern %s.", PATTERN.pattern()), parser, arg);
        }
    }


    private static class PropertiesFileArgumentType implements ArgumentType<Properties> {

        @Override
        public Properties convert(final ArgumentParser parser, final Argument arg, final String value) throws ArgumentParserException {
            final Properties properties = new Properties();
            try(final InputStream in = new FileInputStream(value)) {
                properties.load(in);
            } catch (final FileNotFoundException e) {
                throw new ArgumentParserException(format("Properties file %s does not exist.", value), e, parser, arg);
            } catch (final IOException e) {
                throw new ArgumentParserException(format("Properties file %s could not be read.", value), e, parser, arg);
            }
            return properties;
        }
    }

    private static class FlapMapArgumentAction implements ArgumentAction {

        @Override
        public void run(final ArgumentParser parser, final Argument arg,
                        final Map<String, Object> attrs, final String flag, final Object value) throws ArgumentParserException {
            if (value instanceof Map) {
                for (final Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                    attrs.put(asStringOrNull(entry.getKey()), asStringOrNull(entry.getValue()));
                }
            } else if (value == null) {
                throw new ArgumentParserException(format("%s cannot handle null values!", this.getClass()), parser, arg);
            } else {
                throw new ArgumentParserException(format("%s cannot handle values of type %s. Expects values of type %s.", this.getClass(), value.getClass(), Map.Entry.class), parser, arg);
            }
        }

        @Override
        public boolean consumeArgument() {
            return true;
        }

        @Override
        public void onAttach(final Argument arg) {}

        private String asStringOrNull(final Object value) {
            return value != null ? String.valueOf(value) : null;
        }
    }

}