package cwms.cda.data.dto.timeseriesprofile;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class TimeSeriesProfileTest
{
	@Test
	void testTimeSeriesProfileSerializationRoundTrip() {
		TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile();
		ContentType contentType = Formats.parseHeader(Formats.JSONV2);
		String serialized = Formats.format(contentType, timeSeriesProfile);
		TimeSeriesProfile deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2), serialized, TimeSeriesProfile.class);
		assertEquals(timeSeriesProfile, deserialized, "Roundtrip serialization failed");
		assertEquals(timeSeriesProfile.hashCode(), deserialized.hashCode(), "Roundtrip serialization failed");
	}

	@Test
	void testTimeSeriesProfileSerializationRoundTripFromFile() throws Exception {
		TimeSeriesProfile timeSeriesProfile = buildTestTimeSeriesProfile();
		InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/timeseriesprofile/timeseriesprofile.json");
		assertNotNull(resource);
		String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
		TimeSeriesProfile deserialized = Formats.parseContent(Formats.parseHeader(Formats.JSONV2), serialized, TimeSeriesProfile.class);
		assertEquals(timeSeriesProfile, deserialized, "Roundtrip serialization failed");
	}

//	@Test
//	void testValidate() {
//		Location location = buildTestLocation();
//		String projectId = "project";
//		assertAll(() -> {
//					Embankment embankment = new Embankment.Builder().build();
//					assertThrows(FieldException.class, embankment::validate,
//							"Expected validate() to throw FieldException because Location field can't be null, but it didn't");
//				}, () -> {
//					Embankment embankment = new Embankment.Builder().withLocation(location).build();
//					assertThrows(FieldException.class, embankment::validate,
//							"Expected validate() to throw FieldException because Project Id field can't be null, but it didn't");
//				}, () -> {
//					Embankment embankment = new Embankment.Builder().withLocation(location).withProjectId(projectId).build();
//					assertThrows(FieldException.class, embankment::validate,
//							"Expected validate() to throw FieldException because Project Office Id field can't be null, but it didn't");
//				}, () -> {
//					Embankment embankment = new Embankment.Builder().withLocation(location).withProjectId(projectId).withProjectOfficeId("SPK").build();
//					assertThrows(FieldException.class, embankment::validate,
//							"Expected validate() to throw FieldException because Structure type field can't be null, but it didn't");
//				}
//		);
//	}

	private TimeSeriesProfile buildTestTimeSeriesProfile() {
		return new TimeSeriesProfile.Builder()
				.withOfficeId("Office")
				.withKeyParameter("Depth")
				.withRefTsId("TimeSeries")
				.withLocationId("Location")
				.withDescription("Description")
				.withParameterList(Arrays.asList(new String[]{"Temperature", "Depth"}))
				.build();
	}

}
