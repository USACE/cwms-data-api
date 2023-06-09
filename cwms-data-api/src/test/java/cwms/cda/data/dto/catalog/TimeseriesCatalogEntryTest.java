package cwms.cda.data.dto.catalog;

import java.time.ZonedDateTime;
import java.util.ArrayList;

import io.restassured.path.json.JsonPath;
import io.restassured.path.xml.XmlPath;
import org.junit.jupiter.api.Test;

import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.TimeSeriesExtents;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TimeseriesCatalogEntryTest
{

	@Test
	void test_xml_serialization_earliest(){
		CatalogEntry entry = buildEntry();
		Catalog cat = new Catalog(null, 1, 10,
				new ArrayList<CatalogEntry>(){{add(entry);}});

		ContentType contentType = Formats.parseHeader(Formats.XML);
		String xml = Formats.format(contentType, cat);

		assertNotNull(xml);
		assertFalse(xml.isEmpty());

		XmlPath path = XmlPath.from(xml);

		assertThat(path.getString("catalog.entries.entry.@name"), equalTo("Barren-Lake.Elev.Inst.0.0.USGS-raw"));
		assertThat(path.getString("catalog.entries.entry.units"), equalTo("m"));
		assertThat(path.getInt("catalog.entries.entry.interval"), equalTo(0));
		assertThat(path.getLong("catalog.entries.entry.interval-offset"), equalTo(-2147483648L));
		assertThat(path.getString("catalog.entries.entry.time-zone"), equalTo("US/Central"));
		Object tmp = path.get("catalog.entries.entry.extents");
		System.out.println(tmp.toString());
		assertThat(path.getString("catalog.entries.entry.extents.extents.earliest-time"), equalTo("2017-07-27T05:00:00Z"));
		assertThat(path.getString("catalog.entries.entry.extents.extents.latest-time"), equalTo("2017-11-24T22:30:00Z"));

	}

	@Test
	void test_json_serialization_earliest(){
		CatalogEntry entry = buildEntry();
		Catalog cat = new Catalog(null, 1, 10,
				new ArrayList<CatalogEntry>(){{add(entry);}});

		ContentType contentType = Formats.parseHeader(Formats.JSONV2);
		String json = Formats.format(contentType, cat);

		assertNotNull(json);
		assertFalse(json.isEmpty());

		JsonPath path = JsonPath.from(json);

		assertThat(path.getString("entries[0].name"), equalTo("Barren-Lake.Elev.Inst.0.0.USGS-raw"));
		assertThat(path.getString("entries[0].units"), equalTo("m"));
		assertThat(path.getInt("entries[0].interval"), equalTo(0));
		assertThat(path.getLong("entries[0].interval-offset"), equalTo(-2147483648L));
		assertThat(path.getString("entries[0].time-zone"), equalTo("US/Central"));
		assertThat(path.getString("entries[0].extents[0].earliest-time"), equalTo("2017-07-27T05:00:00Z"));
		assertThat(path.getString("entries[0].extents[0].latest-time"), equalTo("2017-11-24T22:30:00Z"));

	}


	private TimeseriesCatalogEntry buildEntry()
	{
		TimeseriesCatalogEntry.Builder builder = new TimeseriesCatalogEntry.Builder()
				.officeId("LRL")
				.cwmsTsId("Barren-Lake.Elev.Inst.0.0.USGS-raw")
				.units("m")
				.interval("0").intervalOffset(-2147483648L)
				.timeZone("US/Central")
				.withExtent(new TimeSeriesExtents(null,
				                                  ZonedDateTime.parse("2017-07-27T05:00:00Z"),
				                                  ZonedDateTime.parse("2017-11-24T22:30:00Z"),
						ZonedDateTime.parse("2017-11-24T22:30:00Z")));
		return builder
				.build();

	}

}