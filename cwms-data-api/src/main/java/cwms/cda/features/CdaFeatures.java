package cwms.cda.features;

import org.togglz.core.Feature;
import org.togglz.core.annotation.Label;

public enum CdaFeatures implements Feature {
    @Label("Use object-storage backed Blob DAO in BlobController")
    USE_OBJECT_STORAGE_BLOBS
}
