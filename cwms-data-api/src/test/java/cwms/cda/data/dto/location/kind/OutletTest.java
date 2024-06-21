package cwms.cda.data.dto.location.kind;

import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationIdentifier;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class OutletTest
{
	private static final String SPK = "SPK";
	private static final String PROJECT_LOC = "location";
	private static final String OUTLET_LOC = PROJECT_LOC + "-outlet";

	@ParameterizedTest
	@EnumSource(SerializationType.class)
	void test_serialization(SerializationType test)
	{
		Outlet outlet = buildTestOutlet();
		String json = Formats.format(test._contentType, outlet);

		Outlet parsedOutlet = Formats.parseContent(test._contentType, json, Outlet.class);
		assertEquals(outlet, parsedOutlet);
	}

	@Test
	void test_serialize_from_file() throws Exception
	{
		Outlet turbine = buildTestOutlet();
		InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/turbine.json");
		assertNotNull(resource);
		String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
		Outlet deserialized = Formats.parseContent(new ContentType(Formats.JSONV2), serialized, Outlet.class);
		assertEquals(turbine, deserialized, "Roundtrip serialization failed");
	}

	private Outlet buildTestOutlet()
	{
		LocationIdentifier identifier = new LocationIdentifier.Builder()
				.withLocationId(PROJECT_LOC)
				.withOfficeId(SPK)
				.build();
		Location loc = new Location.Builder(SPK, OUTLET_LOC)
				.withLatitude(0.)
				.withLongitude(0.)
				.withPublicName(OUTLET_LOC)
				.withLocationKind("Outlet")
				.withTimeZoneName(ZoneId.of("UTC"))
				.withHorizontalDatum("NAD84")
				.withVerticalDatum("NAVD88")
				.build();
		CharacteristicRef charRef = new CharacteristicRef.Builder()
				.withCharacteristicId("No idea")
				.withOfficeId(SPK)
				.build();

		Outlet outlet = new Outlet.Builder()
				.withProjectId(identifier)
				.withCharacteristicRef(charRef)
				.withLocation(loc)
				.build();
		return outlet;
	}

	enum SerializationType
	{
		JSONV2(Formats.JSONV2),
		XMLV2(Formats.XMLV2),
		;

		final ContentType _contentType;

		SerializationType(String contentType)
		{
			_contentType = new ContentType(contentType);
		}
	}
}