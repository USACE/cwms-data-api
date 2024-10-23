/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.CASCADE_DELETE;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DATE;
import static cwms.cda.api.Controllers.DATUM;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.EFFECTIVE_DATE;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.FORMAT;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.LEVEL_ID;
import static cwms.cda.api.Controllers.LEVEL_ID_MASK;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.TIMEZONE;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.VERSION;
import static cwms.cda.api.Controllers.addDeprecatedContentTypeWarning;
import static cwms.cda.api.Controllers.queryParamAsClass;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dao.LocationLevelsDao;
import cwms.cda.data.dao.LocationLevelsDaoImpl;
import cwms.cda.data.dto.LocationLevel;
import cwms.cda.data.dto.LocationLevels;
import cwms.cda.data.dto.SeasonalValueBean;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.UnsupportedFormatException;
import cwms.cda.helpers.DateUtils;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.http.HttpResponseException;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class LevelsController implements CrudHandler {
    static final String TAG = "Levels";
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    private static final int DEFAULT_PAGE_SIZE = 100;


    public LevelsController(MetricRegistry metrics) {
        this.metrics = metrics;

        requestResultSize = this.metrics.histogram((name(this.getClass().getName(), RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            description = "Create new CWMS Location Level",
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = LocationLevel.class, type = Formats.JSON),
                        @OpenApiContent(from = LocationLevel.class, type = Formats.XML)
                    },
                    required = true),
            method = HttpMethod.POST,
            path = "/levels",
            tags = TAG
    )
    @Override
    public void create(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime(CREATE)) {
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, LocationLevel.class);
            LocationLevel level = Formats.parseContent(contentType, ctx.body(), LocationLevel.class);
            level.validate();

            DSLContext dsl = getDslContext(ctx);
            LocationLevelsDao levelsDao = getLevelsDao(dsl);
            levelsDao.storeLocationLevel(level);
            ctx.status(HttpServletResponse.SC_OK).json("Created Location Level");
        }
    }

    @OpenApi(
            description = "Delete CWMS Location Level",
            pathParams = {
                @OpenApiParam(name = LEVEL_ID, required = true, description = "Specifies the "
                        + "location level id of the Location Level to be deleted"),
            },
            queryParams = {
                @OpenApiParam(name = CASCADE_DELETE, type = Boolean.class, description = "Specifies"
                        + " whether to cascade the delete.  Defaults to false."),
                @OpenApiParam(name = OFFICE, description = "Specifies the owning office of "
                        + "the location level whose data is to be deleted. If this field is "
                        + "not specified, matching location level information will be deleted"
                        + " from all offices."),
                @OpenApiParam(name = EFFECTIVE_DATE, description = "Specifies the "
                        + "effective date of the level to be deleted. If not provided will "
                        + "delete all data and reference to the location level."),
                @OpenApiParam(name = TIMEZONE, description = "Specifies the time zone of "
                        + "the value of the effective date field (unless otherwise "
                        + "specified).If this field is not specified, the default time zone of UTC "
                        + "shall be used."),
            },
            method = HttpMethod.DELETE,
            path = "/levels",
            tags = TAG)
    @Override
    public void delete(@NotNull Context ctx, @NotNull String levelId) {

        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String office = ctx.queryParam(OFFICE);
            String dateString = queryParamAsClass(ctx,
                    new String[]{EFFECTIVE_DATE, DATE}, String.class, null, metrics,
                    name(LevelsController.class.getName(), DELETE));
            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class)
                    .getOrDefault("UTC");
            Boolean cascadeDelete = ctx.queryParamAsClass(CASCADE_DELETE, Boolean.class)
                    .getOrDefault(false);
            ZonedDateTime unmarshalledDateTime = dateString != null
                    ? DateUtils.parseUserDate(dateString, timezone) : null;
            LocationLevelsDao levelsDao = getLevelsDao(dsl);
            levelsDao.deleteLocationLevel(levelId, unmarshalledDateTime, office, cascadeDelete);
            ctx.status(HttpServletResponse.SC_OK).json(levelId + " Deleted");
        }
    }

    @OpenApi(
            queryParams = {
                @OpenApiParam(name = LEVEL_ID_MASK, description = "Specifies the name(s) of "
                        + "the location level(s) whose data is to be included in the response. "
                        + "Uses * for all."),
                @OpenApiParam(name = OFFICE, description = "Specifies the owning "
                        + "office of the location level(s) whose data is to be included in the"
                        + " response. If this field is not specified, matching location level "
                        + "information from all offices shall be returned."),
                @OpenApiParam(name = UNIT, description = "Specifies the unit or unit system"
                        + " of the response. Valid values for the unit field are:"
                        + "\n* `EN`  "
                        + "Specifies English unit system.  Location level values will be in"
                        + " the default English units for their parameters."
                        + "\n* `SI`  "
                        + "Specifies the SI unit system.  Location level values will be in "
                        + "the default SI units for their parameters. "
                        + "\n\nThe default unit system is SI."),
                @OpenApiParam(name = DATUM, description = "Specifies the elevation datum of"
                        + " the response. This field affects only elevation location levels. "
                        + "Valid values for this field are:"
                        + "\n* `NAVD88`  The elevation "
                        + "values will in the specified or default units above the NAVD-88 "
                        + "datum."
                        + "\n* `NGVD29`  The elevation values will be in the "
                        + "specified or default units above the NGVD-29 datum."),
                @OpenApiParam(name = BEGIN, description = "Specifies the start of the time "
                        + "window for data to be included in the response. If this field is "
                        + "not specified, any required time window begins 24 hours prior to "
                        + "the specified or default end time."),
                @OpenApiParam(name = END, description = "Specifies the end of the time "
                        + "window for data to be included in the response. If this field is "
                        + "not specified, any required time window ends at the current time"),
                @OpenApiParam(name = TIMEZONE, description = "Specifies the time zone of "
                        + "the values of the begin and end fields (unless otherwise "
                        + "specified), as well as the time zone of any times in the response."
                        + " If this field is not specified, the default time zone of UTC "
                        + "shall be used."),
                @OpenApiParam(name = FORMAT, description = "Specifies the encoding format "
                        + "of the response. Requests specifying an Accept header:"
                        + Formats.JSONV2 + " must not include this field. "
                        + "Valid format field values for this URI are:"
                        + "\n* `tab`"
                        + "\n* `csv`"
                        + "\n* `xml`"
                        + "\n* `wml2` (only if name field is specified)"
                        + "\n* `json` (default)"),
                @OpenApiParam(name = PAGE, description = "This identifies where in the "
                        + "request you are. This is an opaque value, and can be obtained from "
                        + "the 'next-page' value in the response."),
                @OpenApiParam(name = PAGE_SIZE, type = Integer.class, description = "How "
                        + "many entries per page returned. Default " + DEFAULT_PAGE_SIZE + ".")},
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(type = Formats.JSON),
                    @OpenApiContent(type = ""),
                    @OpenApiContent(from = LocationLevels.class, type = Formats.JSONV2)
                })
            },
            tags = TAG)
    @Override
    public void getAll(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            LocationLevelsDao levelsDao = getLevelsDao(dsl);

            String levelIdMask = queryParamAsClass(ctx, new String[]{LEVEL_ID_MASK, NAME},
                    String.class, null, metrics,
                    name(LevelsController.class.getName(), GET_ALL));

            String office = ctx.queryParam(OFFICE);
            String unit = ctx.queryParamAsClass(UNIT, String.class).getOrDefault(UnitSystem.SI.getValue());
            String datum = ctx.queryParam(DATUM);
            String begin = ctx.queryParam(BEGIN);
            String end = ctx.queryParam(END);

            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class)
                    .getOrDefault("UTC");

            String format = ctx.queryParamAsClass(FORMAT, String.class).getOrDefault("");
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, format, LocationLevels.class);
            String version = contentType.getParameters()
                                          .getOrDefault(VERSION, "");

            boolean isLegacyVersion = version.equals("1");

            if (format.isEmpty() && !isLegacyVersion) {
                String cursor = ctx.queryParamAsClass(PAGE, String.class)
                                   .getOrDefault("");
                int pageSize = ctx.queryParamAsClass(PAGE_SIZE, Integer.class)
                                  .getOrDefault(DEFAULT_PAGE_SIZE);

                ZoneId tz = ZoneId.of(timezone, ZoneId.SHORT_IDS);

                ZonedDateTime endZdt = end != null ? DateUtils.parseUserDate(end, timezone) :
                        ZonedDateTime.now(tz);
                ZonedDateTime beginZdt;
                if (begin != null) {
                    beginZdt = DateUtils.parseUserDate(begin, timezone);
                } else {
                    beginZdt = endZdt.minusHours(24);
                }

                LocationLevels levels = levelsDao.getLocationLevels(cursor, pageSize, levelIdMask,
                        office, unit, datum, beginZdt, endZdt);
                String result = Formats.format(contentType, levels);

                ctx.result(result);
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
                ctx.contentType(contentType.toString());
            } else {
                //Use the type string, not the full string with properties.
                //i.e. application/json not application/json;version=1
                String results = levelsDao.getLocationLevels(format, levelIdMask, office, unit, datum,
                        begin, end, timezone);
                ctx.status(HttpServletResponse.SC_OK);
                ctx.result(results);
                requestResultSize.update(results.length());
                if (isLegacyVersion) {
                    ctx.contentType(contentType.toString());
                } else {
                    ctx.contentType(contentType.getType());
                }
            }
            addDeprecatedContentTypeWarning(ctx, contentType);
        }
    }


    @OpenApi(
            pathParams = {
                @OpenApiParam(name = LEVEL_ID, required = true, description = "Specifies"
                        + " the requested location level."),
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                        + "office of the Location Level to be returned"),
                @OpenApiParam(name = EFFECTIVE_DATE, required = true, description = "Specifies "
                        + "the effective date of Location Level to be returned. "
                        + "Expected formats are `YYYY-MM-DDTHH:MM` or `YYYY-MM-DDTHH:MM:SS`"),
                @OpenApiParam(name = TIMEZONE, description = "Specifies the time zone of "
                        + "the values of the effective date field (unless otherwise "
                        + "specified), as well as the time zone of any times in the response."
                        + " If this field is not specified, the default time zone of UTC "
                        + "shall be used."),
                @OpenApiParam(name = UNIT, description = "Specifies the unit or unit system"
                        + " of the response. Valid values for the unit field are:"
                        + "\n* `EN`  "
                        + "Specifies English unit system.  Location level values will be in"
                        + " the default English units for their parameters."
                        + "\n* `SI`  "
                        + "Specifies the SI unit system.  Location level values will be in "
                        + "the default SI units for their parameters."
                        + "\n* `Other`  "
                        + "Any unit returned in the response to the units URI request that is "
                        + "appropriate for the requested parameters. "),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200,content = {
                    @OpenApiContent(from = LocationLevel.class, type = Formats.JSONV2),
                })
            },
            description = "Retrieves requested Location Level",
            tags = TAG
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String levelId) {
        String office = requiredParam(ctx, OFFICE);
        String units = ctx.queryParam(UNIT);
        String dateString = queryParamAsClass(ctx, new String[]{EFFECTIVE_DATE, DATE},
                String.class, null, metrics, name(LevelsController.class.getName(),
                        GET_ONE));
        String timezone = ctx.queryParamAsClass(TIMEZONE, String.class)
                .getOrDefault("UTC");

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            ZonedDateTime unmarshalledDateTime = DateUtils.parseUserDate(dateString, timezone);

            LocationLevelsDao levelsDao = getLevelsDao(dsl);
            LocationLevel locationLevel = levelsDao.retrieveLocationLevel(levelId,
                    units, unmarshalledDateTime, office);
            ctx.json(locationLevel);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    @OpenApi(
            pathParams = {
                @OpenApiParam(name = LEVEL_ID, required = true, description = "Specifies the "
                        + "location level id of the Location Level to be updated"),
            },
            queryParams = {
                @OpenApiParam(name = EFFECTIVE_DATE, description = "Specifies "
                        + "the effective date of Location Level that will be updated")
            },
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = LocationLevel.class, type = Formats.JSON),
                        @OpenApiContent(from = LocationLevel.class, type = Formats.XML)
                    },
                    required = true),
            description = "Update CWMS Location Level",
            method = HttpMethod.PATCH,
            path = "/levels",
            tags = TAG
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String oldLevelId) {
        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);

            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, LocationLevel.class);
            LocationLevel levelFromBody = Formats.parseContent(contentType, ctx.body(),
                LocationLevel.class);
            String officeId = levelFromBody.getOfficeId();
            if (officeId == null) {
                throw new HttpResponseException(HttpCode.BAD_REQUEST.getStatus(),
                    "The request body must specify the office.");
            }
            String newLevelId = levelFromBody.getLocationLevelId();
            LocationLevelsDao levelsDao = getLevelsDao(dsl);
            if (!oldLevelId.equals(newLevelId)) {
                //if name changed then delete location with old name
                levelsDao.renameLocationLevel(oldLevelId, newLevelId, officeId);
                ctx.status(HttpServletResponse.SC_OK).json("Renamed Location Level");
            } else {
                String dateString = queryParamAsClass(ctx,
                    new String[]{EFFECTIVE_DATE, DATE}, String.class, null, metrics,
                    name(LevelsController.class.getName(), UPDATE));
                if (dateString == null) {
                    throw new IllegalArgumentException("Cannot update location level "
                            + "effective date if no date is specified");
                }
                ZonedDateTime unmarshalledDateTime = DateUtils.parseUserDate(dateString,
                        ZoneId.systemDefault().getId());
                //retrieveLocationLevel will throw an error if level does not exist
                LocationLevel existingLevelLevel = levelsDao.retrieveLocationLevel(oldLevelId,
                    UnitSystem.EN.getValue(), unmarshalledDateTime, officeId);
                existingLevelLevel = updatedClearedFields(ctx.body(), contentType.getType(),
                    existingLevelLevel);
                //only store (update) if level does exist
                LocationLevel updatedLocationLevel = getUpdatedLocationLevel(existingLevelLevel,
                    levelFromBody);
                updatedLocationLevel = new LocationLevel.Builder(updatedLocationLevel)
                    .withLevelDate(unmarshalledDateTime).build();
                levelsDao.storeLocationLevel(updatedLocationLevel);
                ctx.status(HttpServletResponse.SC_OK).json("Updated Location Level");
            }
        } catch (JsonProcessingException ex) {
            throw new FormattingException("Failed to format location level update request", ex);
        }
    }

    private LocationLevel getUpdatedLocationLevel(LocationLevel existingLevel,
                                                  LocationLevel updatedLevel) {
        String seasonalTimeSeriesId = (updatedLevel.getSeasonalTimeSeriesId() == null
                ? existingLevel.getSeasonalTimeSeriesId() : updatedLevel.getSeasonalTimeSeriesId());
        List<SeasonalValueBean> seasonalValues = (updatedLevel.getSeasonalValues() == null
                ? existingLevel.getSeasonalValues() : updatedLevel.getSeasonalValues());
        String specifiedLevelId = (updatedLevel.getSpecifiedLevelId() == null
                ? existingLevel.getSpecifiedLevelId() : updatedLevel.getSpecifiedLevelId());
        String parameterTypeId = (updatedLevel.getParameterTypeId() == null
                ? existingLevel.getParameterTypeId() : updatedLevel.getParameterTypeId());
        String parameterId = (updatedLevel.getParameterId() == null
                ? existingLevel.getParameterId() : updatedLevel.getParameterId());
        Double siParameterUnitsConstantValue = (updatedLevel.getConstantValue() == null
                ? existingLevel.getConstantValue() : updatedLevel.getConstantValue());
        String levelUnitsId = (updatedLevel.getLevelUnitsId() == null
                ? existingLevel.getLevelUnitsId() : updatedLevel.getLevelUnitsId());
        ZonedDateTime levelDate = (updatedLevel.getLevelDate() == null
                ? existingLevel.getLevelDate() : updatedLevel.getLevelDate());
        String levelComment = (updatedLevel.getLevelComment() == null
                ? existingLevel.getLevelComment() : updatedLevel.getLevelComment());
        ZonedDateTime intervalOrigin = (updatedLevel.getIntervalOrigin() == null
                ? existingLevel.getIntervalOrigin() : updatedLevel.getIntervalOrigin());
        Integer intervalMinutes = (updatedLevel.getIntervalMinutes() == null
                ? existingLevel.getIntervalMinutes() : updatedLevel.getIntervalMinutes());
        Integer intervalMonths = (updatedLevel.getIntervalMonths() == null
                ? existingLevel.getIntervalMonths() : updatedLevel.getIntervalMonths());
        String interpolateString = (updatedLevel.getInterpolateString() == null
                ? existingLevel.getInterpolateString() : updatedLevel.getInterpolateString());
        String durationId = (updatedLevel.getDurationId() == null
                ? existingLevel.getDurationId() : updatedLevel.getDurationId());
        BigDecimal attributeValue = (updatedLevel.getAttributeValue() == null
                ? existingLevel.getAttributeValue() : updatedLevel.getAttributeValue());
        String attributeUnitsId = (updatedLevel.getAttributeUnitsId() == null
                ? existingLevel.getAttributeUnitsId() : updatedLevel.getAttributeUnitsId());
        String attributeParameterTypeId = (updatedLevel.getAttributeParameterTypeId() == null
                ? existingLevel.getAttributeParameterTypeId() :
                updatedLevel.getAttributeParameterTypeId());
        String attributeParameterId = (updatedLevel.getAttributeParameterId() == null
                ? existingLevel.getAttributeParameterId() : updatedLevel.getAttributeParameterId());
        String attributeDurationId = (updatedLevel.getAttributeDurationId() == null
                ? existingLevel.getAttributeDurationId() : updatedLevel.getAttributeDurationId());
        String attributeComment = (updatedLevel.getAttributeComment() == null
                ? existingLevel.getAttributeComment() : updatedLevel.getAttributeComment());
        String locationId = (updatedLevel.getLocationLevelId() == null
                ? existingLevel.getLocationLevelId() : updatedLevel.getLocationLevelId());
        String officeId = (updatedLevel.getOfficeId() == null
                ? existingLevel.getOfficeId() : updatedLevel.getOfficeId());
        if (existingLevel.getIntervalMonths() != null && existingLevel.getIntervalMonths() > 0) {
            intervalMinutes = null;
        } else if (existingLevel.getIntervalMinutes() != null
                && existingLevel.getIntervalMinutes() > 0) {
            intervalMonths = null;
        }
        if (existingLevel.getAttributeValue() == null) {
            attributeUnitsId = null;
        }
        if (!existingLevel.getSeasonalValues().isEmpty()) {
            siParameterUnitsConstantValue = null;
            seasonalTimeSeriesId = null;
        } else if (existingLevel.getSeasonalTimeSeriesId() != null
                && !existingLevel.getSeasonalTimeSeriesId().isEmpty()) {
            siParameterUnitsConstantValue = null;
            seasonalValues = null;
        }
        return new LocationLevel.Builder(locationId, levelDate)
                .withSeasonalValues(seasonalValues)
                .withSeasonalTimeSeriesId(seasonalTimeSeriesId)
                .withSpecifiedLevelId(specifiedLevelId)
                .withParameterTypeId(parameterTypeId)
                .withParameterId(parameterId)
                .withConstantValue(siParameterUnitsConstantValue)
                .withLevelUnitsId(levelUnitsId)
                .withLevelComment(levelComment)
                .withIntervalOrigin(intervalOrigin)
                .withIntervalMinutes(intervalMinutes)
                .withIntervalMonths(intervalMonths)
                .withInterpolateString(interpolateString)
                .withDurationId(durationId)
                .withAttributeValue(attributeValue)
                .withAttributeUnitsId(attributeUnitsId)
                .withAttributeParameterTypeId(attributeParameterTypeId)
                .withAttributeParameterId(attributeParameterId)
                .withAttributeDurationId(attributeDurationId)
                .withAttributeComment(attributeComment)
                .withOfficeId(officeId).build();
    }

    public static LocationLevelsDao getLevelsDao(DSLContext dsl) {
        return new LocationLevelsDaoImpl(dsl);
    }

    private static ObjectMapper getObjectMapperForFormat(String format) {
        ObjectMapper om;
        if ((Formats.XML).equals(format)) {
            JacksonXmlModule module = new JacksonXmlModule();
            module.setDefaultUseWrapper(false);
            om = new XmlMapper(module);
        } else if (Formats.JSON.equals(format)) {
            om = new ObjectMapper();
        } else {
            throw new UnsupportedFormatException("Format is not currently supported for Levels: " + format);
        }
        om.registerModule(new JavaTimeModule());
        return om;
    }

    private LocationLevel updatedClearedFields(String body, String format,
                                               LocationLevel existingLevel) throws JsonProcessingException {
        ObjectMapper om = getObjectMapperForFormat(format);
        JsonNode root = om.readTree(body);
        JavaType javaType = om.getTypeFactory().constructType(LocationLevel.class);
        BeanDescription beanDescription = om.getSerializationConfig().introspect(javaType);
        List<BeanPropertyDefinition> properties = beanDescription.findProperties();
        LocationLevel retVal = new LocationLevel.Builder(existingLevel).build();
        try {
            for (BeanPropertyDefinition propertyDefinition : properties) {
                String propertyName = propertyDefinition.getName();
                JsonNode propertyValue = root.findValue(propertyName);
                if (propertyValue != null && "".equals(propertyValue.textValue())) {
                    retVal = new LocationLevel.Builder(retVal)
                                    .withProperty(propertyName, null).build();
                }
            }
        } catch (NullPointerException e) {
            //gets thrown if required field is null
            throw new IllegalArgumentException(e.getMessage());
        }
        return retVal;
    }
}
