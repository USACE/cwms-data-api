package fixtures;

import cwms.cda.features.CdaFeatures;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.togglz.core.context.FeatureContext;
import org.togglz.core.manager.FeatureManager;

public class StackTraceFeatureExtension implements BeforeEachCallback, AfterEachCallback {
    private boolean wasActive;

    @Override
    public void beforeEach(ExtensionContext context) {
        FeatureManager featureManager = FeatureContext.getFeatureManager();
        wasActive = featureManager.isActive(CdaFeatures.INCLUDE_ERROR_STACK_TRACES);
        featureManager.enable(CdaFeatures.INCLUDE_ERROR_STACK_TRACES);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        FeatureManager featureManager = FeatureContext.getFeatureManager();
        if (wasActive) {
            featureManager.enable(CdaFeatures.INCLUDE_ERROR_STACK_TRACES);
        } else {
            featureManager.disable(CdaFeatures.INCLUDE_ERROR_STACK_TRACES);
        }
    }
}
