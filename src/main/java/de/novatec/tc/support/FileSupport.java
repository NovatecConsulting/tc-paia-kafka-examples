package de.novatec.tc.support;

import io.confluent.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class FileSupport {

    private static final Logger LOG = LoggerFactory.getLogger(FileSupport.class);

    private FileSupport() {}

    /**
     * Create a temporary directory.
     */
    public static File tempDirectory(final String prefix) {
        final File file;
        try {
            file = Files.createTempDirectory(prefix).toFile();
            LOG.info("Created temporary directory at {}", file.getAbsolutePath());
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }
        return file;
    }

    public static File cleanUpHook(final File file, final Monitor monitor, final Duration timeout) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if(monitor.await(timeout.toMillis(), MILLISECONDS)) {
                    Utils.delete(file);
                    LOG.info("Deleted temporary file {}", file.getAbsolutePath());
                } else {
                    LOG.warn("Temporary file {} was not deleted, because timeout elapsed.", file.getAbsolutePath());
                }
            } catch (InterruptedException e) {
                LOG.warn("Temporary file {} was not deleted, because clean up was interrupted.", file.getAbsolutePath());
            } catch (IOException e) {
                LOG.warn("Error deleting {}", file.getAbsolutePath());
            }
        }));
        return file;
    }

    public interface Monitor {
        boolean await(long timeout, TimeUnit unit) throws InterruptedException;
    }
}
