package cwms.cda.data.dao;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

@JsonDeserialize(builder = FilteredTimeSeriesParameters.Builder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class FilteredTimeSeriesParameters {
    private final boolean ascending;
    private final Double minValue;
    private final Double maxValue;
    private final boolean filterNulls;

    private FilteredTimeSeriesParameters(Builder builder) {
        this.ascending = builder.ascending;
        this.minValue = builder.minValue;
        this.maxValue = builder.maxValue;
        this.filterNulls = builder.filterNulls;
    }

    public boolean isAscending() {
        return ascending;
    }

    public Double getMinValue() {
        return minValue;
    }

    public Double getMaxValue() {
        return maxValue;
    }

    public boolean isFilterNulls() {
        return filterNulls;
    }

    @JsonPOJOBuilder(withPrefix = "with", buildMethodName = "build")
    public static class Builder {

        private boolean ascending = true;
        private Double minValue;
        private Double maxValue;
        private boolean filterNulls = false;

        public Builder() {
        }

        public Builder withAscending(boolean ascending) {
            this.ascending = ascending;
            return this;
        }

        public Builder withMinValue(Double minValue) {
            this.minValue = minValue;
            return this;
        }

        public Builder withMaxValue(Double maxValue) {
            this.maxValue = maxValue;
            return this;
        }

        public Builder withFilterNulls(boolean filterNulls) {
            this.filterNulls = filterNulls;
            return this;
        }

        public static Builder from(FilteredTimeSeriesParameters params) {
            // This NEEDS to include every field in the FilteredTimeSeriesRequestParameters
            return new Builder()
                    .withAscending(params.ascending)
                    .withMinValue(params.minValue)
                    .withMaxValue(params.maxValue)
                    .withFilterNulls(params.filterNulls);
        }

        public FilteredTimeSeriesParameters build() {
            return new FilteredTimeSeriesParameters(this);
        }
    }
}
