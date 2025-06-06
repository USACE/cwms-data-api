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

package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.api.enums.VersionType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
public final class TimeSeriesWithDataEntryDate extends TimeSeries {

    // Default constructor for Jackson Deserialization
    public TimeSeriesWithDataEntryDate() {
        super();
    }

    // Unused constructor required for Jackson Deserialization
    public TimeSeriesWithDataEntryDate(TimeSeries timeSeries) {
        this(timeSeries.getPage(), timeSeries.getPageSize(), timeSeries.getTotal(), timeSeries.getName(),
                timeSeries.getOfficeId(), timeSeries.getBegin(), timeSeries.getEnd(), timeSeries.getUnits(),
                timeSeries.getInterval(), timeSeries.getVerticalDatumInfo(), timeSeries.getIntervalOffset(),
                timeSeries.getTimeZone(), timeSeries.getVersionDate(), timeSeries.getDateVersionType());
    }

    public TimeSeriesWithDataEntryDate(String page, int pageSize, Integer total, String name, String officeId,
            ZonedDateTime begin, ZonedDateTime end, String units, Duration interval, VerticalDatumInfo info,
            Long intervalOffset, String timeZone, ZonedDateTime versionDate, VersionType dateVersionType) {
        super(page, pageSize, total, name, officeId, begin, end, units, interval, info, intervalOffset,
                timeZone, versionDate, dateVersionType);
    }

    public void addValue(Timestamp dateTime, Double value, int qualityCode, Timestamp dataEntryDate) {
        // Set the current page, if not set
        if ((page == null || page.isEmpty()) && (values == null || values.isEmpty())) {
            page = encodeCursor(String.format("%d", dateTime.getTime()), pageSize, total);
        }
        if (pageSize > 0 && values.size() == pageSize) {
            nextPage = encodeCursor(String.format("%d", dateTime.toInstant().toEpochMilli()), pageSize, total);
        } else {
            values.add(new TimeSeriesRecordWithEntryDate(dateTime, value, qualityCode, dataEntryDate));
        }
    }

    @JsonProperty(value = "value-columns")
    @Override
    public List<Column> getValueColumnsJSON() {
        return getColumnDescriptorWithEntryDate();
    }

    private List<Column> getColumnDescriptorWithEntryDate() {
        List<Column> columns = new ArrayList<>();
        for (Field f: TimeSeries.Record.class.getDeclaredFields()) {
            JsonProperty field = f.getAnnotation(JsonProperty.class);
            if (field != null) {
                String fieldName = !field.value().isEmpty() ? field.value() : f.getName();
                int fieldIndex = field.index();
                columns.add(new TimeSeries.Column(fieldName, fieldIndex + 1, f.getType()));
            }
        }
        for (Field f: TimeSeriesRecordWithEntryDate.class.getDeclaredFields()) {
            JsonProperty field = f.getAnnotation(JsonProperty.class);
            if (field != null) {
                String fieldName = !field.value().isEmpty() ? field.value() : f.getName();
                int fieldIndex = field.index();
                columns.add(new TimeSeries.Column(fieldName, fieldIndex + 1, f.getType()));
            }
        }

        return columns;
    }
}
