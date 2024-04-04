package cwms.cda.data.dao;

import static cwms.cda.api.Controllers.NOT_SUPPORTED_YET;

import cwms.cda.data.dto.forecast.ForecastInstance;
import java.util.List;
import org.jooq.DSLContext;

public class ForecastInstanceDao extends JooqDao<ForecastInstance> {

    public ForecastInstanceDao(DSLContext dsl) {
        super(dsl);
    }

    public void create(ForecastInstance forecastInst) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    public List<ForecastInstance> getForecastInstances(String office, String name, String location) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    public ForecastInstance getForecastInstance(String office, String name, String locationId, String forecastDate, String issueDate) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    public void update(ForecastInstance forecastInst) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    public void delete(String office, String name, String locationId, String forecastDate, String issueDate) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

}
