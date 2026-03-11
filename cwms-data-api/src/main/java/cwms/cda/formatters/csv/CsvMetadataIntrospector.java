package cwms.cda.formatters.csv;

import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector;
import java.lang.annotation.Annotation;

/**
 * Annotation introspector that treats fields/methods annotated with {@link CsvMetadata}
 * as ignorable for serialization when configured with includeMetadata=false. This allows
 * callers to easily exclude dataset-level metadata columns from CSV rows while still using
 * the same DTO class for both modes.
 */
public class CsvMetadataIntrospector extends NopAnnotationIntrospector {

    private final boolean includeMetadata;

    public CsvMetadataIntrospector() {
        this(false);
    }

    public CsvMetadataIntrospector(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    @Override
    public boolean hasIgnoreMarker(AnnotatedMember m) {
        CsvMetadata ann = _findAnnotation(m, CsvMetadata.class);
        if (ann != null) {
            // If metadata columns should not be included, mark them as ignored
            return !includeMetadata;
        }
        return super.hasIgnoreMarker(m);
    }

    @Override
    public boolean isAnnotationBundle(Annotation ann) {
        // This is not an annotation bundle, just a simple marker
        return false;
    }
}
