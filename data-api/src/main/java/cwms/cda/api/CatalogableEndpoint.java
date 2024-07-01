package cwms.cda.api;

import io.swagger.v3.oas.annotations.media.Schema;


@Schema(
        name = "dataSet",
        description = "Set of data for which a larger catalog can be built."
)
public enum CatalogableEndpoint {
    TIMESERIES(Controllers.TIMESERIES),
    LOCATIONS(Controllers.LOCATIONS);

    private String dataSet;

    CatalogableEndpoint(String value) {
        this.dataSet = value;
    }

    public String value() {
        return dataSet;
    }

    public String getValue() {
        return dataSet;
    }

}
