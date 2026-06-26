package cwms.cda.data.dto.csv;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.csv.CsvRows;
import cwms.cda.formatters.csv.CsvV1;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonDeserialize(builder = TimeSeriesCsv.Builder.class)
@FormattableWith(contentType = Formats.CSV, formatter = CsvV1.class, aliases = {Formats.DEFAULT})
public final class TimeSeriesCsv extends CwmsDTOBase implements CwmsCsvDTO<TimeSeriesCsvRow> {

    private final String timeSeriesId;
    private final String officeId;
    private final String versionDate;

    @CsvRows
    private final List<TimeSeriesCsvRow> rows;

    private TimeSeriesCsv(Builder builder) {
        this.timeSeriesId = builder.timeSeriesId;
        this.officeId = builder.officeId;
        this.versionDate = builder.versionDate;
        this.rows = builder.rows;
    }

    public String getTimeSeriesId() { return timeSeriesId; }
    public String getOfficeId() { return officeId; }
    public String getVersionDate() { return versionDate; }

    @Override
    public List<TimeSeriesCsvRow> getRows() {
        return rows;
    }

    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder {
        private String timeSeriesId;
        private String officeId;
        private String versionDate;
        private List<TimeSeriesCsvRow> rows;

        public Builder withTimeSeriesId(String timeSeriesId) {
            this.timeSeriesId = timeSeriesId;
            return this;
        }

        public Builder withOfficeId(String officeId) {
            this.officeId = officeId;
            return this;
        }

        public Builder withVersionDate(String versionDate) {
            this.versionDate = versionDate;
            return this;
        }

        public Builder withRows(List<TimeSeriesCsvRow> rows) {
            this.rows = rows;
            return this;
        }

        public TimeSeriesCsv build() {
            return new TimeSeriesCsv(this);
        }
    }
}
