package cwms.cda.data.dto.location.kind;

import cwms.cda.data.dto.CwmsIdTest;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

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

	@Test
	void test_serialization()
	{
		ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
		Outlet outlet = buildTestOutlet();
		String json = Formats.format(contentType, outlet);

		Outlet parsedOutlet = Formats.parseContent(contentType, json, Outlet.class);
		assertEquals(outlet.getCharacteristicRef(), parsedOutlet.getCharacteristicRef(), "Characteristic refs do not match");
		assertEquals(outlet.getLocation(), parsedOutlet.getLocation(), "Locations do not match");
		CwmsIdTest.assertSame(outlet.getProjectId(), parsedOutlet.getProjectId());
	}

	@Test
	void test_serialize_from_file() throws Exception
	{
		ContentType contentType = Formats.parseHeader(Formats.JSON, Outlet.class);
		Outlet turbine = buildTestOutlet();
		InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/location/kind/outlet.json");
		assertNotNull(resource);
		String serialized = IOUtils.toString(resource, StandardCharsets.UTF_8);
		Outlet deserialized = Formats.parseContent(contentType, serialized, Outlet.class);
		assertEquals(turbine.getCharacteristicRef(), deserialized.getCharacteristicRef(), "Characteristic refs do not match");
		assertEquals(turbine.getLocation(), deserialized.getLocation(), "Locations do not match");
		CwmsIdTest.assertSame(turbine.getProjectId(), deserialized.getProjectId());
	}

	private Outlet buildTestOutlet()
	{
		CwmsId identifier = new CwmsId.Builder()
				.withName(PROJECT_LOC)
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
				.withCharacteristicId("Ogee Weir Depth")
				.withOfficeId(SPK)
				.build();

		Outlet outlet = new Outlet.Builder()
				.withProjectId(identifier)
				.withCharacteristicRef(charRef)
				.withLocation(loc)
				.build();
		return outlet;
	}
}