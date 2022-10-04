package cwms.radar.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.radar.api.errors.FieldException;
import cwms.radar.data.dto.CwmsDTOPaginated;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@JsonDeserialize(builder = RatingMetadataList.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class RatingMetadataList extends CwmsDTOPaginated {

    private List<RatingMetadata> metadata;

    private RatingMetadataList() {

    }


    @JsonIgnore
    public int getSize() {
        int retval = 0;
        if (metadata != null) {
            for (RatingMetadata ratingMetadata : metadata) {
                retval += ratingMetadata.getSize();
            }
        }
        return retval;
    }

    private RatingMetadataList(RatingMetadataList.Builder builder) {
        super(builder.buildPage(), builder.pageSize);
        this.metadata = builder.metadata;
        this.nextPage = builder.buildNextPage();
    }

    public List<RatingMetadata> getRatingMetadata() {
        return Collections.unmodifiableList(metadata);
    }


    @Override
    public void validate() throws FieldException {

    }

    public static class Builder {

        private final int pageSize;

        private int offset;

        private List<RatingMetadata> metadata;

        private boolean isLastPage = false;


        public Builder(int pageSize) {
            this.pageSize = pageSize;
        }

        public Builder withMetadata(Collection<RatingMetadata> col) {
            if (col != null) {
                this.metadata = new ArrayList<>(col);
            } else {
                this.metadata = new ArrayList<>();
            }

            return this;
        }

        public Builder withOffset(int offset) {
            this.offset = offset;
            return this;
        }

        public Builder withIsLastPage(boolean isLastPage) {
            this.isLastPage = isLastPage;
            return this;
        }


        private int getSize() {
            int retval = 0;
            if (metadata != null) {
                for (RatingMetadata ratingMetadata : metadata) {
                    retval += ratingMetadata.getSize();
                }
            }
            return retval;
        }


        public RatingMetadataList build() {
            return new RatingMetadataList(this);
        }


        public String buildPage() {
            // This method needs to return the "page" cursor for this class
            // suitable for passing to the CwmsDTOPaginated constructor
            // CwmsDTOPaginated always puts the pageSize as the last entry in the list
            // we don't need to include it in these results.

            return CwmsDTOPaginated.encodeCursor(offset);
        }

        public String buildNextPage() {
            if (metadata == null || metadata.isEmpty() || isLastPage) {
                return null;
            }
            return CwmsDTOPaginated.encodeCursor(offset + pageSize, pageSize);
        }

    }

}

