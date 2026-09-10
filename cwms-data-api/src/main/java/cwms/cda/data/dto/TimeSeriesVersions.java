/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = TimeSeriesVersions.Builder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@Schema(description = "Represents a list of TimeSeries versions and their extents")
public class TimeSeriesVersions extends CwmsDTOPaginated {
    @Schema(description = "The TimeSeries identifier")
    private final CwmsId tsId;
    @Schema(description = "The list of versions and their extents")
    private final List<TimeSeriesExtents> versions;

    private TimeSeriesVersions(Builder builder) {
        super(builder.page, builder.pageSize, builder.total);
        this.tsId = builder.tsId;
        this.versions = Collections.unmodifiableList(builder.versions);
    }

    public CwmsId getTsId() {
        return tsId;
    }

    public List<TimeSeriesExtents> getVersions() {
        return versions;
    }

    public static class Builder {
        private CwmsId tsId;
        private List<TimeSeriesExtents> versions = new ArrayList<>();
        private String page;
        private int pageSize;
        private Integer total;
        private String nextPage;

        public Builder withTsId(CwmsId tsId) {
            this.tsId = tsId;
            return this;
        }

        public Builder withVersions(List<TimeSeriesExtents> versions) {
            this.versions = new ArrayList<>(versions);
            return this;
        }

        public Builder addVersion(TimeSeriesExtents version) {
            this.versions.add(version);
            return this;
        }

        public Builder withPage(String page) {
            this.page = page;
            return this;
        }

        public Builder withPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder withTotal(Integer total) {
            this.total = total;
            return this;
        }

        public Builder withNextPage(String nextPage) {
            this.nextPage = nextPage;
            return this;
        }

        public TimeSeriesVersions build() {
            TimeSeriesVersions versions = new TimeSeriesVersions(this);
            versions.nextPage = this.nextPage;
            return versions;
        }
    }
}
