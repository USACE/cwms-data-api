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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.data.dao.RatingSpecDao;
import cwms.cda.data.dto.rating.RatingSpec;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import io.javalin.http.Header;
import io.javalin.http.HttpStatus;
import io.javalin.mock.ContextMock;
import io.javalin.router.Endpoint;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;

import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

import static cwms.cda.data.dto.rating.RatingSpecTest.buildRatingSpec;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
		
		Map<String, String> urlParams = new LinkedHashMap<>();
		urlParams.put("office", officeId);

		String paramStr = ControllerTest.buildParamStr(urlParams);		
		var url = "http://127.0.0.1:7001/cwms-data/ratings/spec/" + ratingId;

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
		final var outputStream = new ByteArrayOutputStream();
        var executor = ContextMock.create(config -> {
										config.getReq().contentType = "*";
										config.getReq().requestURL = url;
										config.getReq().queryString = paramStr;
										config.getReq().addHeader(Header.ACCEPT, Formats.JSONV2);
										config.getRes().outputStream = outputStream;
									})
                                 .build("/cwms-data/ratings/spec/{rating-id}");
        var endpoint = Endpoint.create(HandlerType.GET, "/cwms-data/ratings/spec/{rating-id}")
							   .handler(ctx -> controller.getOne(ctx, ratingId));
        var ctx = endpoint.handle(executor);

		// Check that the controller accessed our mock dao in the expected way
		verify(dao, times(1)).retrieveRatingSpec(officeId, ratingId);

		assertEquals(HttpStatus.OK, ctx.status());
		// Make sure controller thought it was happy
		assertEquals(Formats.JSONV2, ctx.res().getContentType());
		
		String result = outputStream.toString(StandardCharsets.UTF_8);
		assertNotNull(result);  // MAke sure we got some sort of response

		// Turn json response back into a spec object
		ObjectMapper om = JsonV2.buildObjectMapper();
		RatingSpec actual = om.readValue(result, RatingSpec.class);

		assertNotNull(actual, () -> result);
	}


}