package cwms.cda.api;

import com.google.common.flogger.FluentLogger;
import cwms.cda.features.CdaFeatures;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;

import org.togglz.core.context.FeatureContext;
import org.togglz.core.manager.FeatureManager;

@Tag("integration")
@ExtendWith(BlobControllerObjectStorageTestIT.FeatureEnableExtension.class)
public class BlobControllerObjectStorageTestIT extends BlobControllerTestIT{
    static FluentLogger logger = FluentLogger.forEnclosingClass();

    public static final String IMAGE_NAME = "minio/minio:RELEASE.2025-04-22T22-12-26Z";
    public static final int PORT = 9000;
    public static final String MINIO_USER = "cda_user";
    public static final String MINIO_USER_SECRET = "cda_password";

    private static final Network NETWORK = Network.newNetwork();
    @Container
    private static final MinIOContainer MINIO_CONTAINER = new MinIOContainer(IMAGE_NAME)
            .withUserName(MINIO_USER)
            .withPassword(MINIO_USER_SECRET);

    public static final String BUCKET = "cwms-test";



    static class FeatureEnableExtension implements Extension, BeforeAllCallback {

        @Override
        public void beforeAll(ExtensionContext context) {
            if (!MINIO_CONTAINER.isRunning()) {
                MINIO_CONTAINER.start();
                createTestBucket();
            }

            setObjectStoreProperties();
        }
    }

    private static void createTestBucket() {
        try (var client = io.minio.MinioClient.builder()
                .endpoint(MINIO_CONTAINER.getS3URL())
                .credentials(MINIO_USER, MINIO_USER_SECRET)
                .build()) {
            if (!client.bucketExists(io.minio.BucketExistsArgs.builder().bucket(BUCKET).build())) {
                client.makeBucket(io.minio.MakeBucketArgs.builder().bucket(BUCKET).build());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test bucket", e);
        }
    }

    static boolean wasActive;

    private static void setObjectStoreProperties() {
        // This is to get the feature enabled before we try and use the controller
        FeatureManager featureManager = FeatureContext.getFeatureManager();

        wasActive=featureManager.isActive(CdaFeatures.USE_OBJECT_STORAGE_BLOBS);
        featureManager.enable(CdaFeatures.USE_OBJECT_STORAGE_BLOBS);
        featureManager.isActive(CdaFeatures.USE_OBJECT_STORAGE_BLOBS);

        String host = MINIO_CONTAINER.getHost();
        Integer port = MINIO_CONTAINER.getMappedPort(PORT);

        System.setProperty("blob.store.endpoint", "http://" + host + ":" + port);
        System.setProperty("blob.store.bucket", BUCKET);
        System.setProperty("blob.store.accessKey", MINIO_USER);
        System.setProperty("blob.store.secretKey", MINIO_USER_SECRET);
    }

    @AfterAll
    public static void teardown() {
        FeatureManager featureManager = FeatureContext.getFeatureManager();
        if(wasActive){
            featureManager.enable(CdaFeatures.USE_OBJECT_STORAGE_BLOBS);
        } else {
            featureManager.disable(CdaFeatures.USE_OBJECT_STORAGE_BLOBS);
        }

    }

}
