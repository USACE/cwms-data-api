package cwms.cda.data.dao;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.javalin.http.Context;
import org.jetbrains.annotations.NotNull;

import static cwms.cda.api.Controllers.ASC;
import static cwms.cda.api.Controllers.FILTER_NULLS;
import static cwms.cda.api.Controllers.MAX_VALUE;
import static cwms.cda.api.Controllers.MIN_VALUE;
import static cwms.cda.api.Controllers.QUERY;

@JsonDeserialize(builder = FilteredTimeSeriesParameters.Builder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class FilteredTimeSeriesParameters {
    private final boolean ascending;
    private final Double minValue;
    private final Double maxValue;
    private final boolean filterNulls;
    private final String query;

    private FilteredTimeSeriesParameters(Builder builder) {
        this.ascending = builder.ascending;
        this.minValue = builder.minValue;
        this.maxValue = builder.maxValue;
        this.filterNulls = builder.filterNulls;
        this.query = builder.query;
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

    public String getQuery() {
        return query;
    }

    @JsonPOJOBuilder(withPrefix = "with", buildMethodName = "build")
    public static class Builder {

        private boolean ascending = true;
        private Double minValue;
        private Double maxValue;
        private boolean filterNulls = false;
        private String query;

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

        public Builder withQuery(String query) {
            this.query = query;
            return this;
        }

        public static Builder from(FilteredTimeSeriesParameters params) {
            // This NEEDS to include every field in the FilteredTimeSeriesRequestParameters
            return new Builder()
                    .withAscending(params.ascending)
                    .withMinValue(params.minValue)
                    .withMaxValue(params.maxValue)
                    .withFilterNulls(params.filterNulls)
                    .withQuery(params.query);
        }

        public static Builder from(@NotNull Context ctx){
            boolean ascending = ctx.queryParamAsClass(ASC, Boolean.class).getOrDefault(true);
            Double minValue = ctx.queryParamAsClass(MIN_VALUE, Double.class).getOrDefault(null);
            Double maxValue = ctx.queryParamAsClass(MAX_VALUE, Double.class).getOrDefault(null);
            boolean filterNulls = ctx.queryParamAsClass(FILTER_NULLS, Boolean.class).getOrDefault(false);
            String query = ctx.queryParamAsClass(QUERY, String.class).getOrDefault(null);

            return new Builder()
                    .withAscending(ascending)
                    .withMinValue(minValue)
                    .withMaxValue(maxValue)
                    .withFilterNulls(filterNulls)
                    .withQuery(query)
                    ;
        }

        public FilteredTimeSeriesParameters build() {
            return new FilteredTimeSeriesParameters(this);
        }
    }
}
