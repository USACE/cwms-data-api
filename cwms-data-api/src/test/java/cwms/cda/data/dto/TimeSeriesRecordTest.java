/*
 *
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.json.JsonV2;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class TimeSeriesRecordTest
{
	@Test
	void testRecordRoundTrip() throws Exception {
		TimeSeries ts = buildTimeSeries();
		TimeSeries.Record tsRecord = ts.values.get(0);

		ObjectMapper om = JsonV2.buildObjectMapper();

		String tsBody = om.writeValueAsString(tsRecord);
		assertNotNull(tsBody);
		JsonNode tsNode = om.readTree(tsBody);
		assertNotNull(tsNode);
		assertNotNull(tsNode.get(0));
		assertEquals(1749211200000L, tsNode.get(0).asLong());
		assertEquals(12.34567, tsNode.get(1).asDouble(), 0.00001);
		assertEquals(0, tsNode.get(2).asInt());
	}

	@Test
	void testRecordWithEntryDateRoundTrip() throws Exception {
		TimeSeries ts = buildTimeSeriesWithEntryDate();
		TimeSeries.Record tsRecord = ts.values.get(0);

		ObjectMapper om = JsonV2.buildObjectMapper();

		String tsBody = om.writeValueAsString(tsRecord);
		assertNotNull(tsBody);
		JsonNode tsNode = om.readTree(tsBody);
		assertNotNull(tsNode);
		assertEquals(1749211200000L, tsNode.get(0).asLong());
		assertEquals(12.34567, tsNode.get(1).asDouble(), 0.00001);
		assertEquals(0, tsNode.get(2).asInt());
	}


	private TimeSeries buildTimeSeries()
	{
		String tsId = "TS-Record-Test.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST";
		ZonedDateTime start = ZonedDateTime.parse("2021-06-21T14:00:00-07:00[PST8PDT]");
		ZonedDateTime end = ZonedDateTime.parse("2021-06-22T14:00:00-07:00[PST8PDT]");
		ZonedDateTime versionDate = Instant.now().atZone(ZoneId.of("UTC"));
		TimeSeries ts = new TimeSeries(null, -1, 0, tsId, "LRL", start, end, null, Duration.ZERO, null, versionDate, null);
		ts.addValue(Timestamp.from(Instant.parse("2025-06-06T12:00:00Z")), 12.34567, 0);
		ts.addValue(Timestamp.from(Instant.parse("2025-06-06T12:00:00Z").plusSeconds(60)), 13.45678, 0);
		return ts;
	}

	private TimeSeries buildTimeSeriesWithEntryDate()
	{
		String tsId = "TS-Record-Test.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST";
		ZonedDateTime start = ZonedDateTime.parse("2021-06-21T14:00:00-07:00[PST8PDT]");
		ZonedDateTime end = ZonedDateTime.parse("2021-06-22T14:00:00-07:00[PST8PDT]");
		ZonedDateTime versionDate = Instant.now().atZone(ZoneId.of("UTC"));
		TimeSeries ts = new TimeSeries(null, -1, 0, tsId, "LRL", start, end, null, Duration.ZERO, null, versionDate, null);
		ts.addValue(Timestamp.from(Instant.parse("2025-06-06T12:00:00Z")), 12.34567, 0, Timestamp.from(Instant.now().minusSeconds(60)));
		ts.addValue(Timestamp.from(Instant.parse("2025-06-06T12:00:00Z").plusSeconds(60)), 13.45678, 0, Timestamp.from(Instant.now()));
		return ts;
	}
}
