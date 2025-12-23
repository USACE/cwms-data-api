package cwms.cda.features;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.togglz.core.manager.FeatureManager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

class CdaFeatureManagerProviderTest {

    private String originalPropertiesFile;

    @BeforeEach
    void setUp() {
        originalPropertiesFile = System.getProperty(CdaFeatureManagerProvider.PROPERTIES_FILE);
    }

    @AfterEach
    void tearDown() {
        if (originalPropertiesFile != null) {
            System.setProperty(CdaFeatureManagerProvider.PROPERTIES_FILE, originalPropertiesFile);
        } else {
            System.clearProperty(CdaFeatureManagerProvider.PROPERTIES_FILE);
        }
    }

    @Test
    void testPriority() {
        CdaFeatureManagerProvider provider = new CdaFeatureManagerProvider();
        assertEquals(10, provider.priority());
    }

    @Test
    void testGetFeatureManager() {
        CdaFeatureManagerProvider provider = new CdaFeatureManagerProvider();
        FeatureManager manager = provider.getFeatureManager();
        assertNotNull(manager);
        assertSame(manager, provider.getFeatureManager(), "Should return the same instance");
    }

    @Test
    void testUseObjectStorageBlobsFeature() throws IOException {
        File tempFile = Files.createTempFile("features", ".properties").toFile();
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write(CdaFeatures.USE_OBJECT_STORAGE_BLOBS.name() + " = true");
        }

        System.setProperty(CdaFeatureManagerProvider.PROPERTIES_FILE, tempFile.getAbsolutePath());

        CdaFeatureManagerProvider provider = new CdaFeatureManagerProvider();
        FeatureManager manager = provider.getFeatureManager();

        assertTrue(manager.isActive(CdaFeatures.USE_OBJECT_STORAGE_BLOBS));
    }

    @Test
    void testFeatureDisabledByDefault() throws IOException {
        File tempFile = Files.createTempFile("features_disabled", ".properties").toFile();
        tempFile.deleteOnExit();

        // Empty file should mean features are disabled by default
        System.setProperty(CdaFeatureManagerProvider.PROPERTIES_FILE, tempFile.getAbsolutePath());

        CdaFeatureManagerProvider provider = new CdaFeatureManagerProvider();
        FeatureManager manager = provider.getFeatureManager();

        assertFalse(manager.isActive(CdaFeatures.USE_OBJECT_STORAGE_BLOBS));
    }
}
