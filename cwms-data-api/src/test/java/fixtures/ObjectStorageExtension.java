package fixtures;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.junit.jupiter.Container;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

/**
 * Sets up a KeyCloak instance to use for testing.
 */
public final class ObjectStorageExtension implements BeforeAllCallback {
    public static final String OBJECT_STORAGE_USER = "cda_user";
    public static final String OBJECT_STORAGE_USER_SECRET = "cda_password";
    public static final String IMAGE_VERSION = "5.2.2";
    public static final int PORT = 9090;

    public static final String BUCKET = "cwms-test";

    @SuppressWarnings("resource") // Managed by this extension
    @Container
    private static final S3MockContainer OBJECT_STORAGE_CONTAINER = new S3MockContainer(IMAGE_VERSION)
                                                                    .withInitialBuckets(BUCKET);


    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (!OBJECT_STORAGE_CONTAINER.isRunning()) {
            OBJECT_STORAGE_CONTAINER.start();
            createTestBucket();
        }

        System.setProperty("blob.store.endpoint", OBJECT_STORAGE_CONTAINER.getHttpEndpoint());
        System.setProperty("blob.store.bucket", BUCKET);
        System.setProperty("blob.store.accessKey", OBJECT_STORAGE_USER);
        System.setProperty("blob.store.secretKey", OBJECT_STORAGE_USER_SECRET);
    }

    private static void createTestBucket() {
        try (var client = io.minio.MinioClient.builder()
                .endpoint(OBJECT_STORAGE_CONTAINER.getHttpEndpoint())
                .credentials(OBJECT_STORAGE_USER, OBJECT_STORAGE_USER_SECRET)
                .build()) {
            if (!client.bucketExists(io.minio.BucketExistsArgs.builder().bucket(BUCKET).build())) {
                client.makeBucket(io.minio.MakeBucketArgs.builder().bucket(BUCKET).build());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test bucket", e);
        }
    }

    public static void shutdown() {
        if (OBJECT_STORAGE_CONTAINER.isRunning()) {
            OBJECT_STORAGE_CONTAINER.stop();
        }
    }


}
