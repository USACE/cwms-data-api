package cwms.cda.features;

import org.togglz.core.Feature;
import org.togglz.core.annotation.Label;

public enum CdaFeatures implements Feature {
    @Label("Use object-storage backed Blob DAO in BlobController")
    USE_OBJECT_STORAGE_BLOBS,

    @Label("Re-enable non-hash key support")
    AUTH_RE_ENABLE_NON_HASH_KEY_SUPPORT,
    @Label("Include stack traces in JSON error responses for authorized debug requests")
    INCLUDE_ERROR_STACK_TRACES,

    @Label("Enable office-scoped reusable user lists")
    USER_LISTS
}
