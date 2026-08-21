package cwms.cda.data.dto.filteredtimeseries;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dao.FilteredTimeSeriesParameters;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;

@JsonRootName("filtered-timeseries")
@JsonPropertyOrder(alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
@FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML})
@JsonIgnoreProperties(ignoreUnknown = true)
public class FilteredTimeSeries extends CwmsDTOPaginated {

    @JsonProperty("time-series")
    private TimeSeries timeSeries;

    @JsonProperty("filter-parameters")
    private FilteredTimeSeriesParameters  filterParams;

    @SuppressWarnings("unused") // required so Jackson can deserialize
    protected FilteredTimeSeries() {
        super();
    }

    public FilteredTimeSeries(TimeSeries timeSeries, FilteredTimeSeriesParameters parameters) {
        super(timeSeries.getPage(), timeSeries.getPageSize(), timeSeries.getTotal());
        this.timeSeries = timeSeries;
        this.filterParams = parameters;
    }

    public TimeSeries getTimeSeries() {
        return timeSeries;
    }

    public FilteredTimeSeriesParameters getFilterParams() {
        return filterParams;
    }

    /**
     * Clears the pagination information (page and nextPage) in the contained TimeSeries object.
     * This is useful when the pagination information should not be exposed or used.
     */
    public void clearTimeSeriesPagination() {
        if (timeSeries != null) {
            timeSeries.clearPagination();
        }
    }
}
