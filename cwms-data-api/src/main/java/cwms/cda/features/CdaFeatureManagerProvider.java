package cwms.cda.features;

import com.google.auto.service.AutoService;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import org.togglz.core.manager.FeatureManager;
import org.togglz.core.manager.FeatureManagerBuilder;
import org.togglz.core.repository.file.FileBasedStateRepository;
import org.togglz.core.spi.FeatureManagerProvider;

@AutoService(FeatureManagerProvider.class)
public class CdaFeatureManagerProvider implements FeatureManagerProvider {
    public static final String DEFAULT_PROPERTIES_FILE = "features.properties";
    public static final String PROPERTIES_FILE = "properties.file";
    private volatile FeatureManager manager;

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public FeatureManager getFeatureManager() {
        if (manager == null) {
            synchronized (this) {
                if (manager == null) {
                    manager = new FeatureManagerBuilder()
                            .featureEnum(CdaFeatures.class)
                            .stateRepository(new FileBasedStateRepository(resolvePropertiesFile()))
                            .build();
                }
            }
        }
        return manager;
    }

    private File resolvePropertiesFile() {
        String configuredFile = System.getProperty(PROPERTIES_FILE);
        if (configuredFile != null && !configuredFile.isBlank()) {
            return new File(configuredFile);
        }

        File defaultFile = new File(DEFAULT_PROPERTIES_FILE);
        if (defaultFile.isFile()) {
            return defaultFile;
        }

        return copyDefaultPropertiesFromClasspath();
    }

    private File copyDefaultPropertiesFromClasspath() {
        ClassLoader classLoader = CdaFeatureManagerProvider.class.getClassLoader();
        try (InputStream input = classLoader.getResourceAsStream(DEFAULT_PROPERTIES_FILE)) {
            if (input == null) {
                return new File(DEFAULT_PROPERTIES_FILE);
            }

            File tempFile = Files.createTempFile("cda-features", ".properties").toFile();
            tempFile.deleteOnExit();
            Files.copy(input, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return tempFile;
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to load default feature properties", ex);
        }
    }
}
