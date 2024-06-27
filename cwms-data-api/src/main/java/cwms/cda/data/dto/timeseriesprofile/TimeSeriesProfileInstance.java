package cwms.cda.data.dto.timeseriesprofile;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;

import java.util.List;

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonDeserialize(builder = TimeSeriesProfileInstance.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class TimeSeriesProfileInstance implements CwmsDTOBase {
    private final TimeSeriesProfile timeSeriesProfile;
    private final List<ProfileTimeSeries> timeSeriesList;

    private TimeSeriesProfileInstance(Builder builder)
    {
        timeSeriesList = builder.timeSeriesList;
        timeSeriesProfile = builder.timeSeriesProfile;
    }

    public TimeSeriesProfile getTimeSeriesProfile() {
        return timeSeriesProfile;
    }

    public List<ProfileTimeSeries> getTimeSeriesList() {
        return timeSeriesList;
    }

    @Override
    public void validate() throws FieldException {

    }
    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder {
        private List<ProfileTimeSeries> timeSeriesList;
        private TimeSeriesProfile timeSeriesProfile;

        public TimeSeriesProfileInstance.Builder withTimeSeriesProfile(TimeSeriesProfile timeSeriesProfile) {
            this.timeSeriesProfile = timeSeriesProfile;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withTimeSeriesList(List<ProfileTimeSeries> timeSeriesList) {
            this.timeSeriesList = timeSeriesList;
            return this;
        }

        public TimeSeriesProfileInstance build() {
            return new TimeSeriesProfileInstance(this);
        }
    }
}
