/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@JsonDeserialize(builder = TimeSeriesIdentifiersByTypeList.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class TimeSeriesIdentifiersByTypeList extends CwmsDTOPaginated {

    private final List<TimeSeriesIdentifiersByType> timeSeriesIdsForLocations;

    private TimeSeriesIdentifiersByTypeList(Builder builder) {
        super(builder.cursor, builder.pageSize, builder.total);
        this.timeSeriesIdsForLocations = new ArrayList<>(builder.timeSeriesIdsForLocations);
    }

    public static String getOffice(String cursor)
    {
        String[] parts = CwmsDTOPaginated.decodeCursor(cursor);
        if (parts.length > 1) {
            String[] idAndOffice = CwmsDTOPaginated.decodeCursor(parts[0]);
            if (idAndOffice.length > 0) {
                return idAndOffice[0];
            }
        }
        return null;
    }

    public static String getId(String cursor) {
        String[] parts = CwmsDTOPaginated.decodeCursor(cursor);
        if (parts.length > 1) {
            String[] idAndOffice = CwmsDTOPaginated.decodeCursor(parts[0]);
            if (idAndOffice.length > 1) {
                return idAndOffice[1];
            }
        }
        return null;
    }

    public List<TimeSeriesIdentifiersByType> getTimeSeriesIdsForLocations() {
        return Collections.unmodifiableList(timeSeriesIdsForLocations);
    }

    public static class Builder {
        private String cursor;
        private int pageSize;
        private Integer total;
        private String nextPage;

        private List<TimeSeriesIdentifiersByType> timeSeriesIdsForLocations = new ArrayList<>();

        public Builder withCursor(String cursor) {
            this.cursor = cursor;
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

        public Builder withTimeSeriesIdsForLocations(Collection<TimeSeriesIdentifiersByType> timeSeriesIdsForLocations) {
            this.timeSeriesIdsForLocations = new ArrayList<>(timeSeriesIdsForLocations);
            return this;
        }

        public TimeSeriesIdentifiersByTypeList build() {
            TimeSeriesIdentifiersByTypeList retval = new TimeSeriesIdentifiersByTypeList(this);

            if (timeSeriesIdsForLocations.size() == pageSize && !timeSeriesIdsForLocations.isEmpty()) {
                TimeSeriesIdentifiersByType lastTimeSeriesIdentifiersByType = timeSeriesIdsForLocations.get(timeSeriesIdsForLocations.size() - 1);
                String cursor = encodeCursor(CwmsDTOPaginated.delimiter, lastTimeSeriesIdentifiersByType.getLocationId().getOfficeId(),
                        lastTimeSeriesIdentifiersByType.getLocationId().getName());
                retval.nextPage = encodeCursor(cursor, pageSize, total);
            } else {
                retval.nextPage = null;
            }
           return retval;
        }
    }
}
