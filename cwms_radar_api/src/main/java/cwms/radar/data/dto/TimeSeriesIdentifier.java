package cwms.radar.data.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import cwms.radar.api.errors.FieldException;
import java.util.Arrays;
import java.util.List;
import mil.army.usace.hec.metadata.Duration;
import mil.army.usace.hec.metadata.Interval;
import mil.army.usace.hec.metadata.Parameter;
import mil.army.usace.hec.metadata.ParameterType;
import mil.army.usace.hec.metadata.Version;

@JsonDeserialize(builder = cwms.radar.data.dto.TimeSeriesIdentifier.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TimeSeriesIdentifier implements CwmsDTO {


    private final LocationID locationId;
    private final cwms.radar.data.dto.Parameter parameter;
    private final cwms.radar.data.dto.ParameterType parameterType;
    private final cwms.radar.data.dto.Duration duration;
    private final String version;
    private final String interval;

    private TimeSeriesIdentifier(Builder builder) {
        this.locationId = builder.locationId;
        this.parameter = builder.parameter;
        this.parameterType = builder.parameterType;
        this.duration = builder.duration;
        this.version = builder.version;
        this.interval = builder.interval;
    }

    @Override
    public void validate() throws FieldException {

    }

    public LocationID getLocationId() {
        return locationId;
    }

    public cwms.radar.data.dto.Parameter getParameter() {
        return parameter;
    }

    public cwms.radar.data.dto.ParameterType getParameterType() {
        return parameterType;
    }

    public cwms.radar.data.dto.Duration getDuration() {
        return duration;
    }

    public String getVersion() {
        return version;
    }

    public String getInterval() {
        return interval;
    }

    @Override
    public String toString() {

        List<String> parts = Arrays.asList(
                getLocationId().toString(),
                getParameter().toString(),
                getParameterType().toString(),
                getInterval(),
                getDuration().toString(),
                getVersion()
        );

        String retval = String.join(".", parts);

        return retval;
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static class Builder {
        private cwms.radar.data.dto.LocationID locationId;
        private cwms.radar.data.dto.Parameter parameter;
        private cwms.radar.data.dto.ParameterType parameterType;
        private cwms.radar.data.dto.Duration duration;
        private String version;
        private String interval;

        public Builder withTimeSeriesIdentifier(mil.army.usace.hec.metadata.timeseries.TimeSeriesIdentifier tsId) {

            mil.army.usace.hec.metadata.LocationID metaLocId = tsId.getLocationId();
            cwms.radar.data.dto.LocationID dtoLocId =
                    new cwms.radar.data.dto.LocationID.Builder().withLocationID(metaLocId).build();
            this.withLocationId(dtoLocId);
            Parameter metaParameter = tsId.getParameter();
            cwms.radar.data.dto.Parameter dtoParameter =
                    new cwms.radar.data.dto.Parameter.Builder().withParameter(metaParameter).build();
            this.withParameter(dtoParameter);

            ParameterType metaParamType = tsId.getParameterType();
            cwms.radar.data.dto.ParameterType dtoParamType =
                    new cwms.radar.data.dto.ParameterType.Builder().withType(metaParamType).build();
            this.withParameterType(dtoParamType);

            Duration metaDuration = tsId.getDuration();
            cwms.radar.data.dto.Duration dtoDuration =
                    new cwms.radar.data.dto.Duration.Builder().withDuration(metaDuration).build();
            this.withDuration(dtoDuration);

            Version metaVersion = tsId.getVersion();
            this.withVersion(metaVersion.getVersion());

            Interval metaInterval = tsId.getInterval();
            this.withInterval(metaInterval.getInterval());

            return this;
        }

        private Builder withInterval(String interval) {
            this.interval = interval;
            return this;
        }

        private Builder withVersion(String ver) {
            this.version = ver;
            return this;
        }

        private Builder withDuration(cwms.radar.data.dto.Duration dur) {
            this.duration = dur;
            return this;

        }

        private Builder withParameterType(cwms.radar.data.dto.ParameterType paramType) {
            this.parameterType = paramType;
            return this;
        }

        private Builder withParameter(cwms.radar.data.dto.Parameter parameter) {
            this.parameter = parameter;
            return this;
        }

        private Builder withLocationId(cwms.radar.data.dto.LocationID locationId) {
            this.locationId = locationId;
            return this;
        }


        public TimeSeriesIdentifier build() {
            return new TimeSeriesIdentifier(this);
        }
    }


}
