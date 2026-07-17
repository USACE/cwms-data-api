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

package cwms.cda.data.dto.measurement;

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

@JsonDeserialize(builder = MeasurementList.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
public class MeasurementList extends CwmsDTOPaginated {
    private final List<Measurement> measurements;

    private MeasurementList(Builder builder) {
        super(builder.cursor, builder.pageSize, builder.total);
        this.measurements = new ArrayList<>(builder.measurements);
    }

    public List<Measurement> getMeasurements() {
        return Collections.unmodifiableList(measurements);
    }

    public static class Builder {
        private String cursor;
        private int pageSize;
        private Integer total;
        private List<Measurement> measurements = new ArrayList<>();

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

        public Builder withMeasurements(Collection<Measurement> measurements) {
            this.measurements = new ArrayList<>(measurements);
            return this;
        }

        public MeasurementList build() {
            MeasurementList retval = new MeasurementList(this);

            if (measurements.size() == pageSize && !measurements.isEmpty()) {
                Measurement last = measurements.get(measurements.size() - 1);
                String cursor = encodeCursor(CwmsDTOPaginated.delimiter, last.getOfficeId(),
                        last.getLocationId(), last.getMeasurementId());
                retval.nextPage = encodeCursor(cursor, pageSize, total);
            } else {
                retval.nextPage = null;
            }
            return retval;
        }
    }
}
