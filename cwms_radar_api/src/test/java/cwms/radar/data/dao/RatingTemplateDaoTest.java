package cwms.radar.data.dao;

import java.sql.SQLException;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.data.dto.rating.RatingTemplate;
import cwms.radar.formatters.json.JsonV2;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static cwms.radar.data.dao.DaoTest.getConnection;
import static cwms.radar.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Disabled
class RatingTemplateDaoTest
{
	public static final String OFFICE_ID = "SWT";

	@Test
	void testRetrieveRatingTemplates() throws SQLException, JsonProcessingException
	{
		try(DSLContext lrl = getDslContext(getConnection(), OFFICE_ID))
		{
			RatingTemplateDao dao = new RatingTemplateDao(lrl);
			Set<RatingTemplate> ratingTemplates = dao.retrieveRatingTemplates( OFFICE_ID, "^Count");
			assertNotNull(ratingTemplates);
			assertFalse(ratingTemplates.isEmpty());

			ObjectMapper objectMapper = JsonV2.buildObjectMapper();
			String body = objectMapper.writeValueAsString(ratingTemplates);
			assertNotNull(body);

		}
	}
}