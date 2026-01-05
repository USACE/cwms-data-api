package cwms.cda.api;

import com.google.common.flogger.FluentLogger;
import cwms.cda.features.CdaFeatures;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.togglz.core.context.FeatureContext;
import org.togglz.core.manager.FeatureManager;

@Tag("integration")
@ExtendWith(BlobControllerObjectStorageTestIT.FeatureEnableExtension.class)
public class BlobControllerObjectStorageTestIT extends BlobControllerTestIT{
    FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String MINIO_ADMIN = "minio_admin";
    public static final String MINIO_ADMIN_SECRET = "saersdbewadfqewrbwreq12rfgweqrffw52354ec";
    public static final String IMAGE_NAME = "minio/minio:RELEASE.2025-04-22T22-12-26Z";
    public static final int PORT = 9000;
    public static final String MINIO_USER = "cda_user";
    public static final String MINIO_USER_SECRET = "cda_password";
    private static final GenericContainer<?> MINIO_CONTAINER = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME))
            .withExposedPorts(PORT)
            .withEnv("MINIO_ROOT_USER", MINIO_ADMIN)
            .withEnv("MINIO_ROOT_PASSWORD", MINIO_ADMIN_SECRET)
            .withCommand("server /data")
            .waitingFor(Wait.forHttp("/minio/health/live").forPort(PORT));

    public static final String BUCKET = "cwms-test";
    public static final String CONTAINER_NAME = "myminio";


    static class FeatureEnableExtension implements Extension, BeforeAllCallback {

        @Override
        public void beforeAll(ExtensionContext context) {
            if (!MINIO_CONTAINER.isRunning()) {
                MINIO_CONTAINER.start();
                setupMinioResources();
            }


            setObjectStoreProperties();
        }
    }

    private static void setupMinioResources() {
        try {
            String address = "http://" + MINIO_CONTAINER.getHost() + ":" + MINIO_CONTAINER.getMappedPort(PORT);
            GenericContainer<?> mc = new GenericContainer<>(DockerImageName.parse("minio/mc:latest"))
                    .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("/bin/sh", "-c"))
                    .withCommand(
                            String.format("mc alias set %s %s %s %s;" , CONTAINER_NAME, address, MINIO_ADMIN, MINIO_ADMIN_SECRET) +
                                    String.format(" mc admin user add %s %s %s;", CONTAINER_NAME, MINIO_USER, MINIO_USER_SECRET) +
                                    String.format(" mc mb --ignore-existing %s/%s;", CONTAINER_NAME, BUCKET) +
                                    String.format(" mc admin policy attach %s readwrite --user %s;", CONTAINER_NAME, MINIO_USER)
                            )
                    .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("minio-mc")))
                    .withAccessToHost(true);
            mc.start();

            long startTime = System.currentTimeMillis();
            while (mc.isRunning() && (System.currentTimeMillis() - startTime) < 10000) {
                Thread.sleep(100);
            }

            if (mc.isRunning()) {
                throw new RuntimeException("MinIO setup timed out after 10 seconds");
            }

            // Check if it exited successfully (0)
            if (mc.getContainerInfo().getState().getExitCodeLong() != 0) {
                throw new RuntimeException("MinIO setup failed with exit code: "
                        + mc.getContainerInfo().getState().getExitCodeLong());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to setup MinIO resources", e);
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
