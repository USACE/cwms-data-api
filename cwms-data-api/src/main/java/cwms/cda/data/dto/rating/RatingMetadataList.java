/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.rating;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@JsonDeserialize(builder = RatingMetadataList.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
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
        if(metadata == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(metadata);
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
