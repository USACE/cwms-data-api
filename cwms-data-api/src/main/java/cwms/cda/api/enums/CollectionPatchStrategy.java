package cwms.cda.api.enums;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(
        name = "Collection Patch Strategy",
        description = CollectionPatchStrategy.DESCRIPTION
)
public enum CollectionPatchStrategy {

    OVERWRITE,
    MERGE;

    public static final String DESCRIPTION = "Controls how a PATCH request body's collection "
            + "fields are applied. OVERWRITE (default): the collection becomes exactly what "
            + "the body contains -- anything within the request's time window that isn't named "
            + "in the body is removed. MERGE: items named in the body are matched to "
            + "existing items by their identity field(s) -- the field(s) marked @Identifier, "
            + "or, when the collection's element type has none of those, whichever field(s) are "
            + "marked @JsonProperty(required = true) -- and updated in place, preserving that "
            + "item's own omitted fields; a null or absent identity field on the incoming item "
            + "never matches anything, so an unmatched identity is added as new, and every other "
            + "existing item is left untouched.";

    public static CollectionPatchStrategy strategyFor(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Cannot determine collection-patch strategy for null or empty");
        }
        String normalized = value.trim().toUpperCase().replace('-', '_');
        for (CollectionPatchStrategy strategy : values()) {
            if (strategy.name().equals(normalized)) {
                return strategy;
            }
        }
        throw new UnsupportedOperationException("Unsupported collection-patch strategy: " + value);
    }
}
