package fixtures;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;

/**
 * Sets up a KeyCloak instance to use for testing.
 */
public final class MinIOExtension implements BeforeAllCallback {

    public static final String MINIO_USER = "cda_user";
    public static final String MINIO_USER_SECRET = "cda_password";
    public static final String IMAGE_NAME = "minio/minio:RELEASE.2025-04-22T22-12-26Z";
    public static final int PORT = 9000;

    public static final String BUCKET = "cwms-test";

    @Container
    private static final MinIOContainer MINIO_CONTAINER = new MinIOContainer(IMAGE_NAME)
            .withUserName(MINIO_USER)
            .withPassword(MINIO_USER_SECRET);


    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (!MINIO_CONTAINER.isRunning()) {
            MINIO_CONTAINER.start();
            createTestBucket();
        }

        String host = MINIO_CONTAINER.getHost();
        Integer port = MINIO_CONTAINER.getMappedPort(PORT);

        System.setProperty("blob.store.endpoint", "http://" + host + ":" + port);
        System.setProperty("blob.store.bucket", BUCKET);
        System.setProperty("blob.store.accessKey", MINIO_USER);
        System.setProperty("blob.store.secretKey", MINIO_USER_SECRET);
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

    public static void shutdown() {
        if (MINIO_CONTAINER.isRunning()) {
            MINIO_CONTAINER.stop();
        }
    }


}
