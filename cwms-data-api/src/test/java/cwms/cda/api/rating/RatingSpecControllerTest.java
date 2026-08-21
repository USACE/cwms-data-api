/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.rating;

import cwms.cda.api.ControllerTest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.data.dao.RatingSpecDao;
import cwms.cda.data.dto.rating.RatingSpec;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

import static cwms.cda.data.dto.rating.RatingSpecTest.buildRatingSpec;
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

		when(request.getHeader(Header.ACCEPT)).thenReturn(Formats.JSONV2);

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
		verify(response).setContentType(Formats.JSONV2);

		String result = ctx.resultString();
		assertNotNull(result);  // MAke sure we got some sort of response

		// Turn json response back into a spec object
		ObjectMapper om = JsonV2.buildObjectMapper();
		RatingSpec actual = om.readValue(result, RatingSpec.class);

		assertNotNull(actual);
	}


}