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
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.JsonFieldsException;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.LocationLevelsDao;
import cwms.cda.data.dao.LocationLevelsDaoImpl;
import cwms.cda.data.dto.LocationLevel;
import cwms.cda.data.dto.LocationLevels;
import cwms.cda.data.dto.SeasonalValueBean;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.xml.adapters.ZonedDateTimeAdapter;
import cwms.cda.helpers.DateUtils;
import hec.data.level.JDomLocationLevelRef;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.core.validation.Validator;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.http.HttpResponseException;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import mil.army.usace.hec.metadata.Interval;
import mil.army.usace.hec.metadata.IntervalFactory;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public class LevelsController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(LevelsController.class.getName());

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    private static final int defaultPageSize = 100;


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
            tags = {"Levels"}
    )
    @Override
    public void create(@NotNull Context ctx) {

        try (final Timer.Context timeContext = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);
            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            if (contentType == null) {
                throw new FormattingException("Format header could not be parsed");
            }
            LocationLevel level = deserializeLocationLevel(ctx.body(), formatHeader);

            if (level.getOfficeId() == null) {
                throw new HttpResponseException(HttpCode.BAD_REQUEST.getStatus(),
                        "The request body must specify the office.");
            }

            ZonedDateTime unmarshalledDateTime = level.getLevelDate(); //getUnmarshalledDateTime

            ZoneId timezoneId = unmarshalledDateTime.getZone();
            if (timezoneId == null) {
                timezoneId = ZoneId.systemDefault();
            }
            level = new LocationLevel.Builder(level).withLevelDate(unmarshalledDateTime).build();
            level.validate();

            LocationLevelsDao levelsDao = getLevelsDao(dsl);
            levelsDao.storeLocationLevel(level, timezoneId);
            ctx.status(HttpServletResponse.SC_ACCEPTED).json("Created Location Level");
        }
    }

    @OpenApi(
            description = "Delete CWMS Location Level",
            pathParams = {
                    @OpenApiParam(name = LEVEL_ID, required = true, description = "Specifies the "
                            + "location level id of the Location Level to be deleted"),
            },
            queryParams = {
                    @OpenApiParam(name = CASCADE_DELETE, type = Boolean.class),
                    @OpenApiParam(name = OFFICE, description = "Specifies the owning office of "
                            + "the location level whose data is to be deleted. If this field is "
                            + "not specified, matching location level information will be deleted"
                            + " from all offices."),
                    @OpenApiParam(name = EFFECTIVE_DATE, description = "Specifies the "
                            + "effective date of the level to be deleted. If not provided will "
                            + "delete all data and reference to the location level.")
                    },
            method = HttpMethod.DELETE,
            path = "/levels",
            tags = {"Levels"})
    @Override
    public void delete(@NotNull Context ctx, @NotNull String levelId) {

        try (final Timer.Context timeContext = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String office = ctx.queryParam(OFFICE);
            String dateString = queryParamAsClass(ctx,
                    new String[]{EFFECTIVE_DATE, DATE}, String.class, null, metrics,
                    name(LevelsController.class.getName(), DELETE));
            Boolean cascadeDelete = Boolean.parseBoolean(ctx.queryParam(CASCADE_DELETE));
            ZonedDateTimeAdapter zonedDateTimeAdapter = new ZonedDateTimeAdapter();
            ZonedDateTime unmarshalledDateTime = dateString != null
                    ? zonedDateTimeAdapter.unmarshal(dateString) : null;
            LocationLevelsDao levelsDao = getLevelsDao(dsl);
            levelsDao.deleteLocationLevel(levelId, unmarshalledDateTime, office, cascadeDelete);
            ctx.status(HttpServletResponse.SC_ACCEPTED).json(levelId + " Deleted");
        } catch (Exception ex) {
            CdaError re = new CdaError("Failed to delete location level");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
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
                            + " of the response. Valid values for the unit field are:\r\n 1. EN. "
                            + "  Specifies English unit system.  Location level values will be in"
                            + " the default English units for their parameters.\r\n2. SI.   "
                            + "Specifies the SI unit system.  Location level values will be in "
                            + "the default SI units for their parameters.\r\n3. Other. Any unit "
                            + "returned in the response to the units URI request that is "
                            + "appropriate for the requested parameters."),
                    @OpenApiParam(name = DATUM, description = "Specifies the elevation datum of"
                            + " the response. This field affects only elevation location levels. "
                            + "Valid values for this field are:\r\n1. NAVD88.  The elevation "
                            + "values will in the specified or default units above the NAVD-88 "
                            + "datum.\r\n2. NGVD29.  The elevation values will be in the "
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
                            + "Valid format field values for this URI are:\r\n"
                            + "1.    tab\r\n"
                            + "2.    csv\r\n"
                            + "3.    xml\r\n"
                            + "4.    wml2 (only if name field is specified)\r\n"
                            + "5.    json (default)\r\n"),
                    @OpenApiParam(name = PAGE, description = "This identifies where in the "
                            + "request you are. This is an opaque value, and can be obtained from "
                            + "the 'next-page' value in the response."),
                    @OpenApiParam(name = PAGE_SIZE, type = Integer.class, description = "How "
                            + "many entries per page returned. Default " + defaultPageSize + ".")},
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(type = Formats.JSON),
                            @OpenApiContent(type = ""),
                            @OpenApiContent(from = LocationLevels.class, type = Formats.JSONV2)
                        }
                    )
            },
            tags = {"Levels"})
    @Override
    public void getAll(Context ctx) {

        try (final Timer.Context timeContext = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            LocationLevelsDao levelsDao = getLevelsDao(dsl);

            String format = ctx.queryParamAsClass(FORMAT, String.class).getOrDefault("");
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, format);
            String version = contentType.getParameters().get(VERSION);

            String levelIdMask = queryParamAsClass(ctx, new String[]{LEVEL_ID_MASK,
                    NAME}, String.class, null, metrics,
                    name(LevelsController.class.getName(), GET_ALL));

            String office = ctx.queryParam(OFFICE);
            String unit = ctx.queryParam(UNIT);
            String datum = ctx.queryParam(DATUM);
            String begin = ctx.queryParam(BEGIN);
            String end = ctx.queryParam(END);

            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class)
                    .getOrDefault("UTC");

            if ("2".equals(version)) {

                String cursor = ctx.queryParamAsClass(PAGE, String.class)
                        .getOrDefault("");
                int pageSize = ctx.queryParamAsClass(PAGE_SIZE, Integer.class)
                        .getOrDefault(defaultPageSize);

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

                ctx.result(result).contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            } else {
                switch (format) {
                    case "json": {
                        ctx.contentType(Formats.JSON);
                        break;
                    }
                    case "tab": {
                        ctx.contentType(Formats.TAB);
                        break;
                    }
                    case "csv": {
                        ctx.contentType(Formats.CSV);
                        break;
                    }
                    case "xml": {
                        ctx.contentType(Formats.XML);
                        break;
                    }
                    case "wml2": {
                        ctx.contentType(Formats.WML2);
                        break;
                    }
                    case "png": // fall next
                    case "jpg": // fall next
                    default: {
                        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED)
                                .json(CdaError.notImplemented());
                    }
                }

                String results = levelsDao.getLocationLevels(format, levelIdMask, office, unit, datum,
                        begin, end, timezone);
                ctx.status(HttpServletResponse.SC_OK);
                ctx.result(results);
                requestResultSize.update(results.length());
            }
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
                            + "the effective date of Location Level to be returned"),
                    @OpenApiParam(name = TIMEZONE, description = "Specifies the time zone of "
                            + "the values of the effective date field (unless otherwise "
                            + "specified), as well as the time zone of any times in the response."
                            + " If this field is not specified, the default time zone of UTC "
                            + "shall be used."),
                    @OpenApiParam(name = UNIT, required = false, description = "Desired unit for "
                            + "the values retrieved.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,content = {
                            @OpenApiContent(from = LocationLevel.class, type = Formats.JSONV2),
                    })
            },
            description = "Retrieves requested Location Level",
            tags = {"Levels"}
    )
    @Override
    public void getOne(Context ctx, @NotNull String levelId) {
        String office = ctx.queryParam(OFFICE);
        String units = ctx.queryParam(UNIT);
        String dateString = queryParamAsClass(ctx, new String[]{EFFECTIVE_DATE, DATE},
                String.class, null, metrics, name(LevelsController.class.getName(),
                        GET_ONE));
        String timezone = ctx.queryParamAsClass(TIMEZONE, String.class)
                .getOrDefault("UTC");

        try (final Timer.Context timeContext = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            ZonedDateTime unmarshalledDateTime = DateUtils.parseUserDate(dateString, timezone);

            LocationLevelsDao levelsDao = getLevelsDao(dsl);
            LocationLevel locationLevel = levelsDao.retrieveLocationLevel(levelId,
                    units, unmarshalledDateTime, office);
            ctx.json(locationLevel);
            ctx.status(HttpServletResponse.SC_OK);
        } catch (NotFoundException e) {
            throw e;
        } catch (Exception ex) {
            CdaError re = new CdaError("Failed to retrieve Location Level request: "
                    + ex.getLocalizedMessage());
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
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
            tags = {"Levels"}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String oldLevelId) {
        try (final Timer.Context timeContext = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);

            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            if (contentType == null) {
                throw new FormattingException("Format header could not be parsed");
            }
            LocationLevel levelFromBody = deserializeLocationLevel(ctx.body(),
                contentType.getType());
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
                ctx.status(HttpServletResponse.SC_ACCEPTED).json("Renamed Location Level");
            } else {
                String dateString = queryParamAsClass(ctx,
                    new String[]{EFFECTIVE_DATE, DATE}, String.class, null, metrics,
                    name(LevelsController.class.getName(), UPDATE));
                if(dateString == null) {
                    throw new IllegalArgumentException("Cannot update location level effective date if no date is specified");
                }
                ZonedDateTime unmarshalledDateTime = DateUtils.parseUserDate(dateString, ZoneId.systemDefault().getId());
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
                levelsDao.storeLocationLevel(updatedLocationLevel, unmarshalledDateTime.getZone());
                ctx.status(HttpServletResponse.SC_ACCEPTED).json("Updated Location Level");
            }
        } catch (JsonProcessingException ex) {
            CdaError re =
                    new CdaError("Failed to process request: " + ex.getLocalizedMessage());
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
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

    public static LocationLevel deserializeLocationLevel(String body, String format) {
        ObjectMapper om = getObjectMapperForFormat(format);

        try {
            return new LocationLevel.Builder(om.readValue(body, LocationLevel.class)).build();
        } catch (JsonProcessingException e) {
            throw new JsonFieldsException(e);
        }

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
            throw new FormattingException("Format is not currently supported for Levels");
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

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = LEVEL_ID, required = true, description = "Specifies"
                            + " the requested location level."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "office of the Location Level to be returned"),
                    @OpenApiParam(name = INTERVAL, description = "Interval time step for"
                            + " the returned time series. Pseudo-regular interval definitions"
                            + " will be treated like local regular. Irregular interval will generate daily time steps."
                            + " Default: 0"),
                    @OpenApiParam(name = BEGIN, description = "Specifies the "
                            + "start of the time window for data to be included in the response. "
                            + "If this field is not specified, any required time window begins 24"
                            + " hours prior to the specified or default end time. The format for "
                            + "this field is ISO 8601 extended, with optional offset and "
                            + "timezone, i.e., '"
                            + DATE_FORMAT + "', e.g., '" + EXAMPLE_DATE + "'."),
                    @OpenApiParam(name = END,  description = "Specifies the "
                            + "end of the time window for data to be included in the response. If"
                            + " this field is not specified, any required time window ends at the"
                            + " current time. The format for this field is ISO 8601 extended, "
                            + "with optional timezone, i.e., '"
                            + DATE_FORMAT + "', e.g., '" + EXAMPLE_DATE + "'."),
                    @OpenApiParam(name = TIMEZONE,  description = "Specifies "
                            + "the time zone of the values of the begin and end fields (unless "
                            + "otherwise specified), as well as the time zone of any times in the"
                            + " response. If this field is not specified, the default time zone "
                            + "of UTC shall be used.\r\nIgnored if begin was specified with "
                            + "offset and timezone."),
                    @OpenApiParam(name = UNIT, required = false, description = "Desired unit for "
                            + "the values retrieved."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            description = "A CWMS Time Series representation of the specified location level.",
                            content = {
                                    @OpenApiContent(from = TimeSeries.class, type = Formats.JSONV2),
                                    @OpenApiContent(from = TimeSeries.class, type = Formats.XMLV2),
                                    @OpenApiContent(from = TimeSeries.class, type = Formats.XML),
                                    @OpenApiContent(from = TimeSeries.class, type = Formats.JSON),
                                    @OpenApiContent(from = TimeSeries.class, type = ""),
                            }
                    ),
                    @OpenApiResponse(status = STATUS_400, description = "Invalid parameter combination"),
                    @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                            + "parameters did not find a timeseries."),
                    @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                            + "implemented")
            },
            description = "Retrieves requested Location Level",
            tags = {"Levels"},
            method = HttpMethod.GET,
            path = "/levels/{level-id}/timeseries"
    )
    public void getLevelAsTimeSeries(Context ctx) {

        try (final Timer.Context timeContext = markAndTime("getLevelAsTimeSeries")) {
            DSLContext dsl = getDslContext(ctx);
            Validator<String> pathParam = ctx.pathParamAsClass(LEVEL_ID, String.class);
            if (!pathParam.hasValue()) {
                throw new IllegalArgumentException(LEVEL_ID + " path parameter can not be null when retrieving levels as time series");
            }
            String levelId = pathParam.get();
            String office = ctx.queryParam(OFFICE);
            String begin = ctx.queryParam(BEGIN);
            String end = ctx.queryParam(END);
            String units = ctx.queryParam(UNIT);
            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");
            String intervalParameter = ctx.queryParamAsClass(INTERVAL, String.class).getOrDefault("0");

            ZoneId tz = ZoneId.of(timezone, ZoneId.SHORT_IDS);
            begin = begin != null ? begin : "PT-24H";

            ZonedDateTime beginZdt = DateUtils.parseUserDate(begin, timezone);
            ZonedDateTime endZdt = end != null
                    ? DateUtils.parseUserDate(end, timezone)
                    : ZonedDateTime.now(tz);

            LocationLevelsDao levelsDao = getLevelsDao(dsl);
            Interval interval = IntervalFactory.findAny(IntervalFactory.equalsName(intervalParameter))
                    .orElseThrow(() -> new IllegalArgumentException("Invalid interval string: " + intervalParameter + " for location level as timeseries"));
            JDomLocationLevelRef levelRef = new JDomLocationLevelRef(office, levelId);
            TimeSeries timeSeries = levelsDao.retrieveLocationLevelAsTimeSeries(levelRef, beginZdt.toInstant(), endZdt.toInstant(), interval, units);
            ctx.json(timeSeries);
            ctx.status(HttpServletResponse.SC_OK);
        }

    }
}
