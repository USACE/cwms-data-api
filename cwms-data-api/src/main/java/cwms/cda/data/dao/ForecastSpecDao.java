package cwms.cda.data.dao;

import static cwms.cda.api.Controllers.NOT_SUPPORTED_YET;

import cwms.cda.data.dto.forecast.ForecastSpec;
import java.util.List;
import org.jooq.DSLContext;

public class ForecastSpecDao extends JooqDao<ForecastSpec> {

    public ForecastSpecDao(DSLContext dsl) {
        super(dsl);
    }


    public void create(ForecastSpec forecastSpec) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);

    }

    public void delete(String office, String specId, String designator) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    public List<ForecastSpec> getForecastSpecs(String office, String specIdRegex,
                                               String designator, String location,
                                               String sourceEntity) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    public ForecastSpec getForecastSpec(String office, String name, String designator) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    public void update(ForecastSpec forecastSpec) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }
}
