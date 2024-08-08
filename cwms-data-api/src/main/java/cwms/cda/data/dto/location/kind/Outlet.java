/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.location.kind;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = Outlet.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonPropertyOrder({"projectId", "location", "ratingCatId", "ratingGroupId", "sharedLocAliasId"})
public final class Outlet extends ProjectStructure {
    public static final String RATING_LOC_GROUP_CATEGORY = "Rating";
    private final CwmsId ratingCategoryId;
    private final CwmsId ratingGroupId;
    private final String sharedLocAliasId;

    private Outlet(Builder builder) {
        super(builder.projectId, builder.location);
        ratingGroupId = builder.ratingGroupId;
        ratingCategoryId = new CwmsId.Builder().withOfficeId(builder.projectId.getOfficeId())
                                               .withName(RATING_LOC_GROUP_CATEGORY)
                                               .build();
        sharedLocAliasId = builder.sharedLocAliasId;
    }

    public CwmsId getRatingGroupId() {
        return ratingGroupId;
    }

    public CwmsId getRatingCategoryId() {
        return ratingCategoryId;
    }

    public String getSharedLocAliasId() {
        return sharedLocAliasId;
    }

    @JsonIgnoreProperties(value = {"rating-category-id"})
    public static final class Builder {
        private CwmsId projectId;
        private Location location;
        private CwmsId ratingGroupId;
        private String sharedLocAliasId;

        public Builder() {
        }

        public Builder(Outlet outlet) {
            projectId = outlet.getProjectId();
            location = outlet.getLocation();
            ratingGroupId = outlet.getRatingGroupId();
            sharedLocAliasId = outlet.getSharedLocAliasId();
        }

        public Outlet build() {
            return new Outlet(this);
        }

        public Builder withProjectId(CwmsId projectIdentifier) {
            this.projectId = projectIdentifier;
            return this;
        }

        public Builder withLocation(Location location) {
            this.location = location;
            return this;
        }

        public Builder withRatingGroupId(CwmsId ratingGroupId) {
            this.ratingGroupId = ratingGroupId;
            return this;
        }

        public Builder withSharedLocAliasId(String sharedLocAliasId) {
            this.sharedLocAliasId = sharedLocAliasId;
            return this;
        }
    }
}
