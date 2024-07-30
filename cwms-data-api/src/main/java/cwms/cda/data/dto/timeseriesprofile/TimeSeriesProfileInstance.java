package cwms.cda.data.dto.timeseriesprofile;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;

import java.time.Instant;
import java.util.List;

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonDeserialize(builder = TimeSeriesProfileInstance.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class TimeSeriesProfileInstance extends CwmsDTOBase {
    private final TimeSeriesProfile timeSeriesProfile;
    private final List<ProfileTimeSeries> timeSeriesList;
    private final String version;
    private final Instant versionDate;
    private final Instant firstDate;
    private final Instant lastDate;

    private TimeSeriesProfileInstance(Builder builder)
    {
        timeSeriesList = builder.timeSeriesList;
        timeSeriesProfile = builder.timeSeriesProfile;
        version = builder.version;
        versionDate = builder.versionDate;
        firstDate = builder.firstDate;
        lastDate = builder.lastDate;
    }

    public TimeSeriesProfile getTimeSeriesProfile() {
        return timeSeriesProfile;
    }

    public List<ProfileTimeSeries> getTimeSeriesList() {
        return timeSeriesList;
    }

    public String getVersion()
    {
        return version;
    }
    public Instant getVersionDate()
    {
        return versionDate;
    }
    public Instant getFirstDate()
    {
        return firstDate;
    }
    public Instant getLastDate()
    {
        return lastDate;
    }
    @Override
    protected void validateInternal(CwmsDTOValidator validator){
        if(timeSeriesProfile==null)
        {
            throw new RequiredFieldException("timeSeriesProfile");
        }
    }
    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder {
        private List<ProfileTimeSeries> timeSeriesList;
        private TimeSeriesProfile timeSeriesProfile;
        private String version;
        private Instant versionDate;
        private Instant firstDate;
        private Instant lastDate;

        public TimeSeriesProfileInstance.Builder withTimeSeriesProfile(TimeSeriesProfile timeSeriesProfile) {
            this.timeSeriesProfile = timeSeriesProfile;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withTimeSeriesList(List<ProfileTimeSeries> timeSeriesList) {
            this.timeSeriesList = timeSeriesList;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withVersion(String version)
        {
            this.version = version;
            return this;
        }
        public TimeSeriesProfileInstance.Builder withVersionDate(Instant instant)
        {
            this.versionDate = instant;
            return this;
        }
        public TimeSeriesProfileInstance.Builder withFirstDate(Instant instant)
        {
            this.firstDate = instant;
            return this;
        }
        public TimeSeriesProfileInstance.Builder withLastDate(Instant instant)
        {
            this.lastDate = instant;
            return this;
        }
        public TimeSeriesProfileInstance build() {
            return new TimeSeriesProfileInstance(this);
        }
    }
}
