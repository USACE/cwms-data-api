package cwms.cda.api;

import cwms.cda.features.CdaFeatures;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.togglz.core.context.FeatureContext;
import org.togglz.core.manager.FeatureManager;

@Tag("integration")
@ExtendWith(BlobControllerObjectStorageTestIT.FeatureEnableExtension.class)
public class BlobControllerObjectStorageTestIT extends BlobControllerTestIT{

    static boolean wasActive;


    // I need this to happen before super.BeforeAll is run so that the create will create into Object-store version
//    @BeforeAll
//    public static void setup() throws Exception {
//        setObjectStoreProperties();
//
//        // now call the method that the super calls.
//        createExistingBlob();
//    }

    private static void setObjectStoreProperties() {
        FeatureManager featureManager = FeatureContext.getFeatureManager();
        wasActive=featureManager.isActive(CdaFeatures.USE_OBJECT_STORAGE_BLOBS);
        featureManager.enable(CdaFeatures.USE_OBJECT_STORAGE_BLOBS);
        featureManager.isActive(CdaFeatures.USE_OBJECT_STORAGE_BLOBS);

        // TODO: Need to figure out a cleaner way to do this
        System.setProperty("blob.store.endpoint", "http://127.0.0.1:9000");
        System.setProperty("blob.store.bucket", "cwms-test");
        System.setProperty("blob.store.accessKey", "cda_user");
        System.setProperty("blob.store.secretKey", "cda_password");
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

    @Override
    @Test
    void test_create_getOne()
    {
        super.test_create_getOne();
    }

    static class FeatureEnableExtension implements Extension, BeforeAllCallback {

        @Override
        public void beforeAll(ExtensionContext context) {
            setObjectStoreProperties();
        }
    }
}
