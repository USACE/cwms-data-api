/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.timeseriesprofile;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.util.List;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@JsonDeserialize(builder = TimeSeriesProfileList.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TimeSeriesProfileList extends CwmsDTOPaginated {
    private final List<TimeSeriesProfile> profileList;

    private TimeSeriesProfileList(Builder builder) {
        super(builder.page, builder.pageSize, builder.total);
        profileList = builder.timeSeriesProfileList;
    }

    public List<TimeSeriesProfile> getProfileList() {
        return profileList;
    }

    public static class Builder {
        private String page;
        private int pageSize;
        private int total;
        private List<TimeSeriesProfile> timeSeriesProfileList;

        public Builder page(String page) {
            this.page = page;
            return this;
        }

        public Builder pageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder total(int total) {
            this.total = total;
            return this;
        }

        public Builder timeSeriesProfileList(List<TimeSeriesProfile> timeSeriesProfileList) {
            this.timeSeriesProfileList = timeSeriesProfileList;
            return this;
        }

        public TimeSeriesProfileList build() {
            return new TimeSeriesProfileList(this);
        }
    }

    public void addValue(long locationCode, TimeSeriesProfile profile, String keyParameter, long prevLocationCode) {
        // Set the current page, if not set
        if ((page == null || page.isEmpty()) && (profileList == null || profileList.isEmpty())) {
            page = encodeCursor(delimiter, String.format("%d", locationCode), keyParameter, total);
        }
        // if the current item will be on a new page, set the next page to the item before it
        if (pageSize > 0 && profileList != null && profileList.size() == pageSize) {
            nextPage = encodeCursor(delimiter, String.format("%d", prevLocationCode),
                    keyParameter, total);
        } else {
            // add the value to the time series profile list
            assert profileList != null;
            profileList.add(profile);
        }
    }
}
