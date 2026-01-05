package cwms.cda.features;

import com.google.auto.service.AutoService;
import org.togglz.core.manager.FeatureManager;
import org.togglz.core.manager.FeatureManagerBuilder;
import org.togglz.core.repository.file.FileBasedStateRepository;
import java.io.File;
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
                    String file = System.getProperty(PROPERTIES_FILE, DEFAULT_PROPERTIES_FILE);
                    manager = new FeatureManagerBuilder()
                            .featureEnum(CdaFeatures.class)
                            .stateRepository(new FileBasedStateRepository(new File(file)))
                            .build();
                }
            }
        }
        return manager;
    }
}
