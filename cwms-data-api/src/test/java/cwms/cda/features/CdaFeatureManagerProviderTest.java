package cwms.cda.features;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.creation.MockSettingsImpl;
import org.togglz.core.context.FeatureContext;
import org.togglz.core.context.StaticFeatureManagerProvider;
import org.togglz.core.manager.FeatureManager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CdaFeatureManagerProviderTest {

    private String originalPropertiesFile;

    @BeforeEach
    void setUp() {
        originalPropertiesFile = System.getProperty(CdaTogglzBootstrap.PROPERTIES_FILE);
    }

    @AfterEach
    void tearDown() {
        if (originalPropertiesFile != null) {
            System.setProperty(CdaTogglzBootstrap.PROPERTIES_FILE, originalPropertiesFile);
        } else {
            System.clearProperty(CdaTogglzBootstrap.PROPERTIES_FILE);
        }
    }

    @Test
    void testGetFeatureManager() throws IOException {
        
        FeatureManager manager = createManager();
        assertNotNull(manager);
        assertSame(manager, FeatureContext.getFeatureManagerOrNull(), "Should return the same instance");
    }

    @Test
    void testLoadsDefaultPropertiesFromClasspath() throws IOException {
        System.clearProperty(CdaTogglzBootstrap.PROPERTIES_FILE);

        FeatureManager manager = createManager();

        assertFalse(manager.isActive(CdaFeatures.USE_OBJECT_STORAGE_BLOBS));
        assertFalse(manager.isActive(CdaFeatures.AUTH_RE_ENABLE_NON_HASH_KEY_SUPPORT));
        assertTrue(manager.isActive(CdaFeatures.INCLUDE_ERROR_STACK_TRACES));
    }

    @Test
    void testUseObjectStorageBlobsFeature() throws IOException {
        File tempFile = Files.createTempFile("features", ".properties").toFile();
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write(CdaFeatures.USE_OBJECT_STORAGE_BLOBS.name() + " = true" + System.lineSeparator());
            writer.write(CdaFeatures.AUTH_RE_ENABLE_NON_HASH_KEY_SUPPORT.name() + " = true");
        }

        System.setProperty(CdaTogglzBootstrap.PROPERTIES_FILE, tempFile.getAbsolutePath());

        FeatureManager manager = createManager();

        assertTrue(manager.isActive(CdaFeatures.USE_OBJECT_STORAGE_BLOBS));
        assertTrue(manager.isActive(CdaFeatures.AUTH_RE_ENABLE_NON_HASH_KEY_SUPPORT));
    }

    @Test
    void testIncludeErrorStackTracesFeature() throws IOException {
        File tempFile = Files.createTempFile("features", ".properties").toFile();
        tempFile.deleteOnExit();

        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write(CdaFeatures.INCLUDE_ERROR_STACK_TRACES.name() + " = true");
        }

        System.setProperty(CdaTogglzBootstrap.PROPERTIES_FILE, tempFile.getAbsolutePath());
        
        FeatureManager manager = createManager();

        assertTrue(manager.isActive(CdaFeatures.INCLUDE_ERROR_STACK_TRACES));
    }

    @Test
    void testFeatureDisabledByDefault() throws IOException {
        File tempFile = Files.createTempFile("features_disabled", ".properties").toFile();
        tempFile.deleteOnExit();

        // Empty file should mean features are disabled by default
        System.setProperty(CdaTogglzBootstrap.PROPERTIES_FILE, tempFile.getAbsolutePath());

        FeatureManager manager = createManager();

        assertFalse(manager.isActive(CdaFeatures.USE_OBJECT_STORAGE_BLOBS));
        assertFalse(manager.isActive(CdaFeatures.AUTH_RE_ENABLE_NON_HASH_KEY_SUPPORT));
        assertFalse(manager.isActive(CdaFeatures.INCLUDE_ERROR_STACK_TRACES));
    }

    private FeatureManager createManager() throws IOException {
        var ds = mock(DataSource.class);
        var conn = mock(Connection.class, RETURNS_MOCKS);
        
        try {
            when(ds.getConnection()).thenReturn(conn);
        } catch (SQLException ex) {
            throw new IOException("Should not happen", ex);
        }

        FeatureManager manager = new CdaTogglzBootstrap(ds).createFeatureManager();        
        StaticFeatureManagerProvider.setFeatureManager(manager);
        return manager;
    }
}
