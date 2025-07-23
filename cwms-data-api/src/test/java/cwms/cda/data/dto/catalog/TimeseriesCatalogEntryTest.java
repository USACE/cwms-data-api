package cwms.cda.data.dto.catalog;

import cwms.cda.formatters.json.JsonV2;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayList;

import io.restassured.path.json.JsonPath;
import io.restassured.path.xml.XmlPath;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.io.IOUtils;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

		ContentType contentType = Formats.parseHeader(Formats.XML, Catalog.class);
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
		assertThat(path.getString("catalog.entries.entry.extents.extents.earliest-time"), equalTo("2017-07-27T05:00:00Z"));
		assertThat(path.getString("catalog.entries.entry.extents.extents.latest-time"), equalTo("2017-11-24T22:30:00Z"));
		assertThat(path.getString("catalog.entries.entry.aliases.alias[0].@name"), equalTo("alias1"));
		assertThat(path.getString("catalog.entries.entry.aliases.alias[0].value"), equalTo("value1"));
		assertThat(path.getString("catalog.entries.entry.aliases.alias[1].@name"), equalTo("alias2"));
		assertThat(path.getString("catalog.entries.entry.aliases.alias[1].value"), equalTo("value2"));

	}

	@Test
	void test_json_serialization_earliest(){
		CatalogEntry entry = buildEntry();
		Catalog cat = new Catalog(null, 1, 10,
				new ArrayList<CatalogEntry>(){{add(entry);}});

		ContentType contentType = Formats.parseHeader(Formats.JSONV2, Catalog.class);
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
		assertThat(path.getString("entries[0].aliases[0].name"), equalTo("alias1"));
		assertThat(path.getString("entries[0].aliases[0].value"), equalTo("value1"));
		assertThat(path.getString("entries[0].aliases[1].name"), equalTo("alias2"));
		assertThat(path.getString("entries[0].aliases[1].value"), equalTo("value2"));

	}

	@Test
	void test_json_serialization_no_cursor(){
		CatalogEntry entry = buildEntry();
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
		CatalogEntry entry = buildEntry();
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

	@Test
	void test_json_deserialization() throws IOException {
		InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/data/dto/time-series-catalog-entry.json");
		assertNotNull(resource);
		String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
		TimeseriesCatalogEntry deserialized = JsonV2.buildObjectMapper().readValue(json, TimeseriesCatalogEntry.class);
		assertEquals("Pine Flat-Outflow.Stage.Inst.15Minutes.0.one", deserialized.getName());
		assertEquals("SPK", deserialized.getOffice());
		assertEquals("m", deserialized.getUnits());
		assertEquals("15Minutes", deserialized.getInterval());
		Collection<TimeSeriesAlias> aliases = deserialized.getAliases();
		assertEquals(1, aliases.size());
		TimeSeriesAlias alias = aliases.iterator().next();
		assertEquals("Test Category-LessThan3", alias.getName());
		assertEquals("test alias 1", alias.getValue());
	}

	private TimeseriesCatalogEntry buildEntry()
	{
		TimeseriesCatalogEntry.Builder builder = new TimeseriesCatalogEntry.Builder()
				.officeId("LRL")
				.cwmsTsId("Barren-Lake.Elev.Inst.0.0.USGS-raw")
				.units("m")
				.interval("0").intervalOffset(-2147483648L)
				.timeZone("US/Central")
				.withExtent(new TimeSeriesExtents.Builder()
						.withEarliestTime(ZonedDateTime.parse("2017-07-27T05:00:00Z"))
						.withLatestTime(ZonedDateTime.parse("2017-11-24T22:30:00Z"))
						.withLastUpdate(ZonedDateTime.parse("2017-11-24T22:30:00Z"))
						.build())
				.withAliases(Arrays.asList(
						new TimeSeriesAlias.Builder().withName("alias1").withValue("value1").build(),
						new TimeSeriesAlias.Builder().withName("alias2").withValue("value2").build()));
		return builder
				.build();

	}

}