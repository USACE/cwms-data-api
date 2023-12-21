package cwms.cda.data.dao;

import static cwms.cda.data.dao.DaoTest.getConnection;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dto.rating.RatingSpec;
import cwms.cda.formatters.json.JsonV2;
import java.sql.SQLException;
import java.util.Collection;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
class RatingSpecDaoTest {

    public static final String OFFICE_ID = "SWT";

    @Test
    void testRetrieveRatingSpecs() throws SQLException, JsonProcessingException {
        DSLContext lrl = getDslContext(getConnection(), OFFICE_ID);

        RatingSpecDao dao = new RatingSpecDao(lrl);
        Collection<RatingSpec> ratingSpecs = dao.retrieveRatingSpecs(OFFICE_ID, "^ARTH");
        assertNotNull(ratingSpecs);
        assertFalse(ratingSpecs.isEmpty());

        ObjectMapper objectMapper = JsonV2.buildObjectMapper();
        String body = objectMapper.writeValueAsString(ratingSpecs);
        assertNotNull(body);

    }


}