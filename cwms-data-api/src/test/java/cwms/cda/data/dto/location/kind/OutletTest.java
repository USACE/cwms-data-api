package cwms.cda.data.dto.location.kind;

import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationIdentifier;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OutletTest
{
	private static final String SPK = "SPK";
	private static final String PROJECT_LOC = "location";
	private static final String OUTLET_LOC = PROJECT_LOC + "-outlet";

	@ParameterizedTest
	@EnumSource(SerializationType.class)
	void test_serialization(SerializationType test)
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
				.withStructureLocation(loc)
				.withProjectIdentifier(identifier)
				.withCharacteristicRef(charRef)
				.build();
		String json = Formats.format(test._contentType, outlet);

		Outlet parsedOutlet = Formats.parseContent(test._contentType, json, Outlet.class);
		assertEquals(outlet, parsedOutlet);
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