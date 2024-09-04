package cwms.cda.data.dto.catalog;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cwms.cda.data.dto.Catalog;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

class LocationCatalogEntryTest
{

	@Test
	void test_json_serialization_no_cursor(){
		LocationCatalogEntry entry = buildEntry();
		Catalog cat = new Catalog(null, 1, 10,
				new ArrayList<CatalogEntry>(){{add(entry);}});

		ContentType contentType = Formats.parseHeader(Formats.JSONV2, Catalog.class);
		String json = Formats.format(contentType, cat);

		assertNotNull(json);
		assertFalse(json.isEmpty());

		assertFalse(json.contains("cursor"));
	}

	@Test
	void test_xml_serialization_no_cursor() {
		LocationCatalogEntry entry = buildEntry();
		Catalog cat = new Catalog(null, 1, 10,
				new ArrayList<CatalogEntry>() {{
					add(entry);
				}});

		ContentType contentType = Formats.parseHeader(Formats.XML, Catalog.class);
		String xml = Formats.format(contentType, cat);

		assertNotNull(xml);
		assertFalse(xml.isEmpty());
		assertFalse(xml.contains("cursor"));
	}


	private LocationCatalogEntry buildEntry()
	{
		LocationCatalogEntry.Builder builder = new LocationCatalogEntry.Builder()
				.officeId("SPK")
				.name("Test7")
				.nearestCity("Davis")
				.publicName("Resource Management Associates")
				.longName("Resource Management Associates - Water Resources Engineering")
				.description("At RMA, our team of engineers and software developers create and apply advanced numerical models and software systems that support water resource management and environmental stewardship.")
				.kind("SITE")
				.type("Contractor")
				.timeZone("America/Los_Angeles")
				.latitude(38.563258)
				.longitude(-121.730321)
				.publishedLatitude(38.56)
				.publishedLongitude(-121.73)
				.horizontalDatum("NAD83")
				.elevation(41.3)
				.unit("m")
				.verticalDatum("NGVD29")
				.nation("UNITED STATES")
				.state("CA")
				.county("Yolo")
				.boundingOffice("SPD")
				.mapLabel("RMA")
				.active(true)
				.aliases(new ArrayList<>());

		return builder
				.build();

	}

}