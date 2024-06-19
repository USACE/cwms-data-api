package cwms.cda.data.dto.timeseriesprofile;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import fixtures.CwmsDataApiSetupCallback;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimeSeriesProfileParserTest
{
	@Test
	void testTimeSeriesProfileSerializationRoundTrip() throws JsonProcessingException
	{
		TimeSeriesProfileParser timeSeriesProfileParser = buildTestTimeSeriesProfileParser();
		ContentType contentType = Formats.parseHeader(Formats.JSONV2);

		ObjectMapper om = JsonV2.buildObjectMapper();
		String serializedLocation = om.writeValueAsString(timeSeriesProfileParser);

		String serialized = Formats.format(contentType, timeSeriesProfileParser);
		TimeSeriesProfileParser deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2), serialized, TimeSeriesProfileParser.class);
		assertEquals(timeSeriesProfileParser, deserialized, "Roundtrip serialization failed");
		assertEquals( timeSeriesProfileParser.hashCode(), deserialized.hashCode(),
				"Roundtrip serialization failed");
	}
	private static TimeSeriesProfileParser buildTestTimeSeriesProfileParser() {
		List<ParameterInfo> parameterInfo= new ArrayList<>();
		parameterInfo.add (new ParameterInfo.Builder()
				.withParameter("Depth")
				.withIndex(3)
				.withUnit("m")
				.build());
		parameterInfo.add (new ParameterInfo.Builder()
				.withParameter("Temp-Water")
				.withIndex(5)
				.withUnit("F")
				.build());
		return

				new TimeSeriesProfileParser.Builder()
						.withOfficeId("SWT")
						.withLocationId("TIMESERIESPROFILE_LOC")
						.withKeyParameter("Depth")
						.withRecordDelimiter((char) 10)
						.withFieldDelimiter(',')
						.withTimeFormat("MM/DD/YYYY,HH24:MI:SS")
						.withTimeZone("UTC")
						.withTimeField(1)
						.withTimeInTwoFields(false)
						.withParameterInfoList(parameterInfo)
						.build();
	}

}
