package cwms.cda.formatters;


import static org.junit.jupiter.api.Assertions.*;

import cwms.cda.api.enums.VersionType;
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.data.dto.Blob;
import cwms.cda.data.dto.Blobs;
import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.Clob;
import cwms.cda.data.dto.Clobs;
import cwms.cda.data.dto.County;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.locationlevel.LocationLevels;
import cwms.cda.data.dto.Office;
import cwms.cda.data.dto.RecentValue;
import cwms.cda.data.dto.State;
import cwms.cda.data.dto.basinconnectivity.Basin;
import cwms.cda.data.dto.project.LockRevokerRights;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.texttimeseries.RegularTextTimeSeriesRow;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2Office;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;


class FormatsTest {

    public static final String FIREFOX_HEADER = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8";

    @Test
    void testParsePatchContentPreservesFieldsOmittedFromBody() {
        TextTimeSeries existing = new TextTimeSeries.Builder()
                .withOfficeId("SPK")
                .withName("TsTestLoc.Flow.Inst.1Hour.0.raw")
                .withTimeZone("UTC")
                .withDateVersionType(VersionType.UNVERSIONED)
                .build();

        // Body only contains rows -- no office-id, name, time-zone, or date-version-type.
        String patchBody = "{\"regular-text-values\":[{\"date-time\":\"2024-01-01T00:00:00Z\","
                + "\"text-value\":\"updated\"}]}";

        ContentType contentType = Formats.parseHeader("application/json;version=2", TextTimeSeries.class);
        TextTimeSeries patched = Formats.parsePatchContent(contentType, existing, patchBody, TextTimeSeries.class);

        assertEquals("SPK", patched.getOfficeId(), "office-id omitted from body should be preserved");
        assertEquals("TsTestLoc.Flow.Inst.1Hour.0.raw", patched.getName(), "name omitted from body should be preserved");
        assertEquals("UTC", patched.getTimeZone(), "time-zone omitted from body should be preserved");
        assertEquals(VersionType.UNVERSIONED, patched.getDateVersionType());
        assertNotNull(patched.getRegularTextValues());
        assertEquals(1, patched.getRegularTextValues().size());
        assertEquals("updated", patched.getRegularTextValues().iterator().next().getTextValue());
    }

    @Test
    void testParsePatchContentArrayReplaced() {
        List<RegularTextTimeSeriesRow> existingRows = new ArrayList<>();
        existingRows.add(new RegularTextTimeSeriesRow.Builder()
                .withDateTime(Instant.parse("2024-01-01T00:00:00Z"))
                .withTextValue("original one")
                .build());
        existingRows.add(new RegularTextTimeSeriesRow.Builder()
                .withDateTime(Instant.parse("2024-01-01T01:00:00Z"))
                .withTextValue("original two")
                .build());
        TextTimeSeries existing = new TextTimeSeries.Builder()
                .withOfficeId("SPK")
                .withName("TsTestLoc.Flow.Inst.1Hour.0.raw")
                .withTimeZone("UTC")
                .withDateVersionType(VersionType.UNVERSIONED)
                .withRegularTextValues(existingRows)
                .build();

        // Body only contains rows -- no office-id, name, time-zone, or date-version-type.
        String patchBody = "{\"regular-text-values\":[{\"date-time\":\"2024-01-01T00:00:00Z\","
                + "\"text-value\":\"updated\"}]}";

        ContentType contentType = Formats.parseHeader("application/json;version=2", TextTimeSeries.class);
        TextTimeSeries patched = Formats.parsePatchContent(contentType, existing, patchBody, TextTimeSeries.class);

        assertEquals("SPK", patched.getOfficeId(), "office-id omitted from body should be preserved");
        assertEquals("TsTestLoc.Flow.Inst.1Hour.0.raw", patched.getName(), "name omitted from body should be preserved");
        assertEquals("UTC", patched.getTimeZone(), "time-zone omitted from body should be preserved");
        assertEquals(VersionType.UNVERSIONED, patched.getDateVersionType());
        assertNotNull(patched.getRegularTextValues());
        assertEquals(1, patched.getRegularTextValues().size());
        assertEquals("updated", patched.getRegularTextValues().iterator().next().getTextValue());
    }

    @Test
    void testParsePatchContentExplicitNullClearsField() {
        TextTimeSeries existing = new TextTimeSeries.Builder()
                .withOfficeId("SPK")
                .withName("TsTestLoc.Flow.Inst.1Hour.0.raw")
                .withTimeZone("UTC")
                .withVersionDate(Instant.parse("2024-01-01T00:00:00Z"))
                .build();

        // Explicitly clears version-date while leaving time-zone untouched.
        String patchBody = "{\"version-date\":null}";
        ContentType contentType = Formats.parseHeader("application/json;version=2", TextTimeSeries.class);
        TextTimeSeries patched = Formats.parsePatchContent(contentType, existing, patchBody, TextTimeSeries.class);

        assertNull(patched.getVersionDate(), "explicit null in the body should clear the field");
        assertEquals("UTC", patched.getTimeZone(), "fields omitted from the body should remain unchanged");
    }

    @Test
    void testParsePatchContentFullPayloadBehavesLikeANormalParse() {
        TextTimeSeries existing = new TextTimeSeries.Builder().build();

        String patchBody = "{\"office-id\":\"SPK\",\"name\":\"TsTestLoc.Flow.Inst.1Hour.0.raw\","
                + "\"time-zone\":\"UTC\",\"regular-text-values\":["
                + "{\"date-time\":\"2024-01-01T00:00:00Z\",\"text-value\":\"v\"}]}";

        ContentType contentType = Formats.parseHeader("application/json;version=2", TextTimeSeries.class);
        TextTimeSeries patched = Formats.parsePatchContent(contentType, existing, patchBody, TextTimeSeries.class);

        assertEquals("SPK", patched.getOfficeId());
        assertEquals("TsTestLoc.Flow.Inst.1Hour.0.raw", patched.getName());
        assertEquals("UTC", patched.getTimeZone());
        assertEquals(1, patched.getRegularTextValues().size());
    }

    @Test
    void testParsePatchContentMissingRequiredFieldFailsValidation() {
        // No office-id anywhere -- not on existing, and not in the body.
        TextTimeSeries existing = new TextTimeSeries.Builder().withName("SomeName").build();

        String patchBody = "{\"time-zone\":\"UTC\"}";
        ContentType contentType = Formats.parseHeader("application/json;version=2", TextTimeSeries.class);

        assertThrows(RequiredFieldException.class,
                () -> Formats.parsePatchContent(contentType, existing, patchBody, TextTimeSeries.class));
    }

    @Test
    void testParsePatchContentReplacesArrayInsteadOfAppending() {
        // Reproduces the bug this guards against: patching one row of a timeseries that
        // already has several rows must not leave the old rows sitting alongside the newly
        // patched one. Jackson's default JsonNode-tree merge behavior appends incoming array
        // elements onto whatever's already there instead of replacing the array -- that's what
        // Formats#applyJsonMergePatch's setDefaultMergeable(false) is specifically for.
        TextTimeSeries existing = new TextTimeSeries.Builder()
                .withOfficeId("SPK")
                .withName("TsTestLoc.Flow.Inst.1Hour.0.raw")
                .withTimeZone("UTC")
                .withRegRow(new RegularTextTimeSeriesRow.Builder()
                        .withDateTime(Instant.parse("2024-01-01T00:00:00Z"))
                        .withTextValue("original one")
                        .build())
                .withRegRow(new RegularTextTimeSeriesRow.Builder()
                        .withDateTime(Instant.parse("2024-01-01T01:00:00Z"))
                        .withTextValue("original two")
                        .build())
                .build();

        // Patches only the first row's text-value; the second row isn't mentioned at all.
        String patchBody = "{\"regular-text-values\":[{\"date-time\":\"2024-01-01T00:00:00Z\","
                + "\"text-value\":\"patched\"}]}";

        ContentType contentType = Formats.parseHeader("application/json;version=2", TextTimeSeries.class);
        TextTimeSeries patched = Formats.parsePatchContent(contentType, existing, patchBody, TextTimeSeries.class);

        assertNotNull(patched.getRegularTextValues());
        assertEquals(1, patched.getRegularTextValues().size(),
                "regular-text-values should be replaced by the body's own rows, not appended to");
        assertEquals("patched", patched.getRegularTextValues().iterator().next().getTextValue());
    }

    @Test
    void testParseHeaderAndQueryParmJson() {
        ContentType contentType = Formats.parseHeaderAndQueryParm("application/json", null, LocationLevels.class);
        assertNotNull(contentType);
        assertEquals("application/json", contentType.getType());
        Map<String, String> parameters = contentType.getParameters();
        assertEquals("2", parameters.get("version"));
    }

    @Test
    void testParseHeaderAndQueryParmJsonV2() {
        ContentType contentType = Formats.parseHeaderAndQueryParm("application/json;version=2",
            null, LocationLevels.class);

        assertNotNull(contentType);
        assertEquals("application/json", contentType.getType());
        Map<String, String> parameters = contentType.getParameters();
        assertNotNull(parameters);
        assertFalse(parameters.isEmpty());
        assertTrue(parameters.containsKey("version"));
        assertEquals("2", parameters.get("version"));
    }

    @Test
    void testParseNullNull() {
        assertThrows(FormattingException.class, () -> Formats.parseHeaderAndQueryParm(null, null, Catalog.class));
    }

    @Test
    void testParseEmptyHeader() {

        ContentType contentType = Formats.parseHeaderAndQueryParm("", "json", LocationLevels.class);

        assertNotNull(contentType);
        assertEquals("application/json", contentType.getType());
        Map<String, String> parameters = contentType.getParameters();
        assertEquals("2", parameters.get("version"));
    }

    @Test
    void testParseNullHeader() {

        ContentType contentType = Formats.parseHeaderAndQueryParm(null, "json", Catalog.class);

        assertNotNull(contentType);
        assertEquals("application/json", contentType.getType());
        Map<String, String> parameters = contentType.getParameters();
        assertEquals("1", parameters.get("version"));
    }


    @Test
    void testParseHeaderAndQueryParmXml() {
        assertThrows(FormattingException.class, () -> {
            Formats.parseHeaderAndQueryParm(null, null, Catalog.class);
        });

        ContentType contentType = Formats.parseHeaderAndQueryParm("application/xml", null, Catalog.class);

        assertNotNull(contentType);
        assertEquals("application/xml", contentType.getType());
        Map<String, String> parameters = contentType.getParameters();
        assertTrue(parameters == null || parameters.isEmpty());

        /** xml;version=2 is not a supported format of Catalog */
        assertThrows(UnsupportedFormatException.class, 
                     () -> Formats.parseHeaderAndQueryParm("application/xml;version=2", null, Catalog.class));
    }

    @Test
    void testParseHeader() {
        ContentType contentType;

        contentType = Formats.parseHeader("application/json", Catalog.class);
        assertNotNull(contentType);
        assertEquals("application/json", contentType.getType());

        contentType = Formats.parseHeader("application/json;version=1", Catalog.class);
        assertNotNull(contentType);
        assertEquals("application/json", contentType.getType());

        contentType = Formats.parseHeader("application/json;version=2", Catalog.class);
        assertNotNull(contentType);
        assertEquals("application/json", contentType.getType());

        assertEquals(new ContentType("application/json;version=1"),
            Formats.parseHeader(null, Catalog.class));
        assertEquals(new ContentType("application/json;version=1"),
            Formats.parseHeader("", Catalog.class));
        assertEquals(new ContentType("application/json;version=1"),
            Formats.parseHeader(" ", Catalog.class));
        assertEquals(new ContentType("application/json;version=2"),
            Formats.parseHeader("application/json;version=2,hello=world", LocationLevels.class));

        assertThrows(FormattingException.class, () -> Formats.parseHeader("abc", CwmsDTOBase.class));
        assertThrows(FormattingException.class, () -> Formats.parseHeader("abc", Catalog.class));
        assertThrows(FormattingException.class, () -> Formats.parseHeader("abc,def", Catalog.class));

        contentType = Formats.parseHeader("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", RecentValue.class);
        assertEquals("application/json", contentType.getType());
    }

    @ParameterizedTest
    @EnumSource(ParseQueryOrParamTest.class)
    void test_header_or_query_parm(ParseQueryOrParamTest test) {
        ContentType ct = Formats.parseQueryOrHeaderParam(test.header, test.query, test.dto);
        System.out.println(ct.toString());
        assertTrue(ContentType.equivalent(ct.toString(), test.type), "In correct content type returned.");
    }

    @EnumSource(ParseQueryParamTest.class)
    @ParameterizedTest
    void testParseQueryParam(ParseQueryParamTest test) {
        ContentType contentType = Formats.parseQueryParam(test.contentType, test.klass);
        assertEquals(test.expectedType, contentType);
    }

    @Test
    void testParseHeaderAndQueryParmJsonV2WithCharset() {
        ContentType contentType = Formats.parseHeaderAndQueryParm("application/json;version=2; charset=utf-8", null,
            LocationLevels.class);

        assertNotNull(contentType);
        assertEquals("application/json", contentType.getType());
    }

    @Test
    void testParseHeaderJsonV2WithCharset() {
        ContentType contentType = Formats.parseHeader("application/json;version=2; charset=utf-8", Catalog.class);

        assertNotNull(contentType);
        assertEquals("application/json", contentType.getType());
    }

    @Test
    void testParseHeaderFromFirefox() {
        //The following header comes from firefox
        ContentType contentType = Formats.parseHeader(FIREFOX_HEADER, Catalog.class);
        assertNotNull(contentType);
        assertEquals(Formats.XML, contentType.toString());
    }

    @EnumSource(ParseHeaderClassAliasTest.class)
    @ParameterizedTest
    void testParseHeaderWithClass(ParseHeaderClassAliasTest test) {
        ContentType contentType = Formats.parseHeader(test.contentType, test.klass);
        assertNotNull(contentType);
        assertEquals(test.expectedType, contentType.toString());
    }

    enum ParseHeaderClassAliasTest {
        COUNTY_DEFAULT(County.class, Formats.DEFAULT, Formats.JSONV2),
        COUNTY_JSON(County.class, Formats.JSON, Formats.JSONV2),
        COUNTY_JSONV2(County.class, Formats.JSONV2, Formats.JSONV2),
        STATE_DEFAULT(State.class, Formats.DEFAULT, Formats.JSONV2),
        STATE_JSON(State.class, Formats.JSON, Formats.JSONV2),
        STATE_JSONV2(State.class, Formats.JSONV2, Formats.JSONV2),
        OFFICE_DEFAULT(Office.class, Formats.JSONV2, Formats.JSONV2),
        OFFICE_JSON(Office.class, Formats.JSONV2, Formats.JSONV2),
        OFFICE_JSONV2(Office.class, Formats.JSONV2, Formats.JSONV2),
        OFFICE_XML(Office.class, Formats.XML, Formats.XMLV2),
        OFFICE_XMLV2(Office.class, Formats.XMLV2, Formats.XMLV2),
        BLOB_DEFAULT(Blob.class, Formats.DEFAULT, Formats.JSONV2),
        BLOB_JSON(Blob.class, Formats.JSON, Formats.JSONV2),
        BLOB_JSONV2(Blob.class, Formats.JSONV2, Formats.JSONV2),
        BLOBS_DEFAULT(Blobs.class, Formats.DEFAULT, Formats.JSONV2),
        BLOBS_JSON(Blobs.class, Formats.JSON, Formats.JSONV2),
        BLOBS_JSONV2(Blobs.class, Formats.JSONV2, Formats.JSONV2),
        CLOB_DEFAULT(Clob.class, Formats.DEFAULT, Formats.JSONV2),
        CLOB_JSON(Clob.class, Formats.JSON, Formats.JSONV2),
        CLOB_JSONV1(Clob.class, Formats.JSONV1, Formats.JSONV1),
        CLOB_JSONV2(Clob.class, Formats.JSONV2, Formats.JSONV2),
        CLOB_XML(Clob.class, Formats.XML, Formats.XMLV2),
        CLOB_XMLV2(Clob.class, Formats.XMLV2, Formats.XMLV2),
        CLOBS_DEFAULT(Clobs.class, Formats.DEFAULT, Formats.JSONV2),
        CLOBS_JSON(Clobs.class, Formats.JSON, Formats.JSONV2),
        CLOBS_JSONV2(Clobs.class, Formats.JSONV2, Formats.JSONV2),
        BASIN_DEFAULT(Basin.class, Formats.DEFAULT, Formats.NAMED_PGJSON),
        BASIN_PGJSON(Basin.class, Formats.PGJSON, Formats.PGJSON),
        BASIN_NAMED_PGJSON(Basin.class, Formats.NAMED_PGJSON, Formats.NAMED_PGJSON),
        PROJECT_JSONV1(Project.class, Formats.JSONV1, Formats.JSONV1),
        PROJECT_JSON(Project.class, Formats.JSON, Formats.JSONV1),
      	LOCK_REVOKER_RIGHTS_JSON(LockRevokerRights.class, Formats.JSON, Formats.JSONV1),
        LOCK_REVOKER_RIGHTS_JSONV1(LockRevokerRights.class, Formats.JSONV1, Formats.JSONV1),
        LOCK_REVOKER_RIGHTS_DEFAULT(LockRevokerRights.class, Formats.DEFAULT, Formats.JSONV1)
        ;

        final Class<? extends CwmsDTOBase> klass;
        final String contentType;
        final String expectedType;

        ParseHeaderClassAliasTest(Class<? extends CwmsDTOBase> theClass, String contentType, String expectedType) {
            klass = theClass;
            this.contentType = contentType;
            this.expectedType = expectedType;
        }
    }

    enum ParseQueryParamTest {
        JSON(null, "json", new ContentType(Formats.JSON)),
        NULL(null, null, null),
        EMPTY(null, "", null),
        OFFICE(Office.class, "json", new ContentType(Formats.JSONV2)),
        ;
        final Class<? extends CwmsDTOBase> klass;
        final String contentType;
        final ContentType expectedType;

        ParseQueryParamTest(Class<? extends CwmsDTOBase> theClass, String contentType, ContentType expectedType) {
            klass = theClass;
            this.contentType = contentType;
            this.expectedType = expectedType;
        }
    }


    enum ParseQueryOrParamTest {
        BOTH(Formats.XML, Formats.CSV_LEGACY, Office.class, Formats.CSV),
        HEADER(Formats.JSON, null, Office.class, Formats.JSONV2),
        QUERY(null, Formats.TAB_LEGACY, Office.class, Formats.TAB)
        ;

        final String header;
        final String query;
        final Class<? extends CwmsDTOBase> dto;
        final String type;

        ParseQueryOrParamTest(String header, String query, Class<? extends CwmsDTOBase> dto, String type)
        {
            this.header = header;
            this.query = query;
            this.dto = dto;
            this.type = type;
        }

    }


    @ParameterizedTest
    @EnumSource(ContentTypeFormatterSource.class)
    void test_formatter_retrieval(ContentTypeFormatterSource test) {
        ContentType ct = Formats.parseHeader(test.contentType, test.dto);
        OutputFormatter formatterActual = Formats.getOutputFormatter(ct, test.dto);
        assertNotNull(formatterActual, "No formatters available for given Content-Type and DTO.");
        assertEquals(test.formatter, formatterActual.getClass(), "Expected Formatter was not returned.");
    }

    public enum ContentTypeFormatterSource {
        OFFICE_DEFAULT("*/*", Office.class, JsonV2.class),
        OFFICE_FIREFOX_DEFAULT("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", Office.class, XMLv2Office.class)

        ;
        final String contentType;
        final Class<? extends CwmsDTOBase> dto;
        final Class<? extends OutputFormatter> formatter;

        ContentTypeFormatterSource(String contentType, Class<? extends CwmsDTOBase> dto, Class<? extends OutputFormatter> formatter) {
            this.contentType = contentType;
            this.dto = dto;
            this.formatter = formatter;
        }
    }
}