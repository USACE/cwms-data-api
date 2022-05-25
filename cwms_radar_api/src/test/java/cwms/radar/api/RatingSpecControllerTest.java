package cwms.radar.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.data.dao.RatingSpecDao;
import cwms.radar.data.dto.rating.RatingSpec;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.json.JsonV1;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

import static cwms.radar.data.dto.rating.RatingSpecTest.buildRatingSpec;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RatingSpecControllerTest
{


	@Test
	void getOne() throws JsonProcessingException
	{
		String officeId = "SWT";
		String ratingId = "ARBU.Elev;Stor.Linear.Production";

		RatingSpec expected = buildRatingSpec(officeId, ratingId);

		// build a mock dao that returns a pre-built ts when called a certain way
		RatingSpecDao dao = mock(RatingSpecDao.class);

		when(dao.retrieveRatingSpec(officeId, ratingId)).thenReturn(Optional.of(expected));

		// build mock request and response
		final HttpServletRequest request= mock(HttpServletRequest.class);
		final HttpServletResponse response = mock(HttpServletResponse.class);
		final Map<String, ?> map = new LinkedHashMap<>();

		when(request.getAttribute("office")).thenReturn(officeId);
		when(request.getAttribute("rating-id")).thenReturn(ratingId);

		when(request.getHeader(Header.ACCEPT)).thenReturn(Formats.JSON);

		Map<String, String> urlParams = new LinkedHashMap<>();
		urlParams.put("office", officeId);
		urlParams.put("rating-id", ratingId);

		String paramStr = ControllerTest.buildParamStr(urlParams);

		when(request.getQueryString()).thenReturn(paramStr);
		when(request.getRequestURL()).thenReturn(new StringBuffer( "http://127.0.0.1:7001/ratings/spec/"));



		// build real context that uses the mock request/response
		Context ctx = new Context(request, response, map);

		// Build a controller that doesn't actually talk to database
		RatingSpecController controller = new RatingSpecController(new MetricRegistry()){
			@Override
			protected DSLContext getDslContext(Context ctx) {
				return null;
			}

			@NotNull
			@Override
			protected RatingSpecDao getRatingSpecDao(DSLContext dsl) {
				return dao;
			}
		};
		// make controller use our mock dao

		// Do a controller getAll with our context
		controller.getOne(ctx, ratingId);

		// Check that the controller accessed our mock dao in the expected way
		verify(dao, times(1)).retrieveRatingSpec(officeId, ratingId);

		// Make sure controller thought it was happy
		verify(response).setStatus(200);
		// And make sure controller returned json
		verify(response).setContentType(Formats.JSON);

		String result = ctx.resultString();
		assertNotNull(result);  // MAke sure we got some sort of response

		// Turn json response back into a spec object
		ObjectMapper om = JsonV1.buildObjectMapper();
		RatingSpec actual = om.readValue(result, RatingSpec.class);

		assertNotNull(actual);
	}


}