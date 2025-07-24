package cwms.cda.data.dao;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.javalin.http.Context;
import org.jetbrains.annotations.NotNull;

import static cwms.cda.api.Controllers.QUERY;

@JsonDeserialize(builder = FilteredTimeSeriesParameters.Builder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class FilteredTimeSeriesParameters {

    private final String query;

    private FilteredTimeSeriesParameters(Builder builder) {
        this.query = builder.query;
    }

    public String getQuery() {
        return query;
    }

    @JsonPOJOBuilder(withPrefix = "with", buildMethodName = "build")
    public static class Builder {
        private String query;

        public Builder() {
        }

        public Builder withQuery(String query) {
            this.query = query;
            return this;
        }

        public static Builder from(FilteredTimeSeriesParameters params) {
            // This NEEDS to include every field in the FilteredTimeSeriesRequestParameters
            return new Builder()
                    .withQuery(params.query);
        }

        public static Builder from(@NotNull Context ctx){
            String query = ctx.queryParamAsClass(QUERY, String.class).getOrDefault(null);

            return new Builder()
                    .withQuery(query)
                    ;
        }

        public FilteredTimeSeriesParameters build() {
            return new FilteredTimeSeriesParameters(this);
        }
    }
}
