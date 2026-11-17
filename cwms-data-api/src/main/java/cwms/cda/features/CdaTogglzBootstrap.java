package cwms.cda.features;

import com.google.auto.service.AutoService;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import javax.sql.DataSource;

import org.togglz.core.bootstrap.TogglzBootstrap;
import org.togglz.core.manager.FeatureManager;
import org.togglz.core.manager.FeatureManagerBuilder;
import org.togglz.core.repository.composite.CompositeStateRepository;
import org.togglz.core.repository.file.FileBasedStateRepository;
import org.togglz.core.repository.jdbc.JDBCStateRepository;

@AutoService(TogglzBootstrap.class)
public class CdaTogglzBootstrap implements TogglzBootstrap {
    public static final String DEFAULT_PROPERTIES_FILE = "features.properties";
    public static final String PROPERTIES_FILE = "properties.file";
    private volatile FeatureManager manager;
    private final DataSource dataSource;

    public CdaTogglzBootstrap(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    @Override
    public FeatureManager createFeatureManager() {
        if (manager == null) {
            synchronized (this) {
                if (manager == null) {
                    var jdbcBuilder = JDBCStateRepository.newBuilder(dataSource).createTable(false);
                    var composite = new CompositeStateRepository(
                                    jdbcBuilder.build(),
                                    new FileBasedStateRepository(resolvePropertiesFile())
                                );
                    // we only set things in the database
                    composite.setSetterSelection(CompositeStateRepository.SetterSelection.FIRST);
                    manager = new FeatureManagerBuilder()
                            .featureEnum(CdaFeatures.class)
                            .stateRepository(composite)
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
        ClassLoader classLoader = CdaTogglzBootstrap.class.getClassLoader();
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
