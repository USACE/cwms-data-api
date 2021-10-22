package cwms.radar.api;

import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.*;
import static cwms.radar.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Timer;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.CwmsDataManager;
import cwms.radar.data.dao.*;
import cwms.radar.data.dto.LocationLevel;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import cwms.radar.formatters.xml.adapters.ZonedDateTimeAdapter;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.*;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class LevelsController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(UnitsController.class.getName());
    private final MetricRegistry metrics;
    private final Meter getAllRequests;
    private final Timer getAllRequestsTime;
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Histogram requestResultSize;
    private final Meter createRequest;
    private final Timer createRequestTime;
    private final Meter deleteRequest;
    private final Timer deleteRequestTime;
    private final Meter updateRequest;
    private final Timer updateRequestTime;

    public LevelsController(MetricRegistry metrics){
        this.metrics=metrics;
        String className = this.getClass().getName();
        getAllRequests = this.metrics.meter(name(className,"getAll","count"));
        getAllRequestsTime = this.metrics.timer(name(className,"getAll","time"));
        getOneRequest = this.metrics.meter(name(className,"getOne","count"));
        getOneRequestTime = this.metrics.timer(name(className,"getOne","time"));
        createRequest = this.metrics.meter(name(className,"create","count"));
        createRequestTime = this.metrics.timer(name(className,"create","time"));
        deleteRequest = this.metrics.meter(name(className,"delete","count"));
        deleteRequestTime = this.metrics.timer(name(className,"delete","time"));
        updateRequest = this.metrics.meter(name(className,"update","count"));
        updateRequestTime = this.metrics.timer(name(className,"update","time"));
        requestResultSize = this.metrics.histogram((name(className,"results","size")));
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name="office", required = true, description="Specifies the office in which Location Level will be created")
            },
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = LocationLevel.class, type = Formats.JSON )
                    },
                    required = true),
            description = "Create new CWMS Location Level",
            method = HttpMethod.POST,
            path = "/levels",
            tags = {"Levels"}
    )
    @Override
    public void create(@NotNull Context ctx)
    {
        createRequest.mark();
        try(final Timer.Context timeContext = createRequestTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            LocationLevelsDao levelsDao = new LocationLevelsDaoImpl(dsl);
            String office = ctx.queryParam("office");
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            if(contentType == null)
            {
                throw new FormattingException("Format header could not be parsed");
            }
            LocationLevel level = deserializeLocationLevel(ctx.body(),formatHeader, office);
            ObjectMapper om = getObjectMapperForFormat(contentType.getType());
            JsonNode root = om.readTree(ctx.body());
            String dateString = root.findValue("level-date").toString().replace("\"", "");
            ZonedDateTimeAdapter zonedDateTimeAdapter = new ZonedDateTimeAdapter();
            ZonedDateTime unmarshalledDateTime = zonedDateTimeAdapter.unmarshal(dateString);
            ZoneId timezoneId = unmarshalledDateTime.getZone();
            if(timezoneId == null)
            {
                timezoneId = ZoneId.systemDefault();
            }
            level.setLevelDate(unmarshalledDateTime);
            levelsDao.storeLocationLevel(level, timezoneId);
            ctx.status(HttpServletResponse.SC_ACCEPTED).json("Created Location Level");
        }
        catch(Exception ex)
        {
            RadarError re = new RadarError("failed to process request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = "cascade-delete", type = Boolean.class),
                    @OpenApiParam(name = "office", description = "Specifies the owning office of the location level whose data is to be deleted. If this field is not specified, matching location level information will be deleted from all offices."),
                    @OpenApiParam(name = "date", description = "Specifies the effective date of the level to be deleted")
            },
            description = "Delete CWMS Location Level",
            method = HttpMethod.DELETE,
            path = "/levels",
            tags = {"Levels"}
    )
    @Override
    public void delete(Context ctx, String id)
    {
        deleteRequest.mark();
        try(final Timer.Context timeContext = deleteRequestTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            String office = ctx.queryParam("office");
            String dateString = ctx.queryParam("date");
            Boolean cascadeDelete = Boolean.parseBoolean(ctx.queryParam("cascade-delete"));
            ZonedDateTimeAdapter zonedDateTimeAdapter = new ZonedDateTimeAdapter();
            ZonedDateTime unmarshalledDateTime = zonedDateTimeAdapter.unmarshal(dateString);
            LocationLevelsDao levelsDao = new LocationLevelsDaoImpl(dsl);
            levelsDao.deleteLocationLevel(id, unmarshalledDateTime, office, cascadeDelete);
            ctx.status(HttpServletResponse.SC_ACCEPTED).json(id + " Deleted");
        }
        catch(Exception ex)
        {
            RadarError re = new RadarError("Failed to delete location level");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name="name", required=false, description="Specifies the name(s) of the location level(s) whose data is to be included in the response. Uses * for all."),
            @OpenApiParam(name="office", required=false, description="Specifies the owning office of the location level(s) whose data is to be included in the response. If this field is not specified, matching location level information from all offices shall be returned."),
            @OpenApiParam(name="unit", required=false, description="Specifies the unit or unit system of the response. Valid values for the unit field are:\r\n 1. EN.   Specifies English unit system.  Location level values will be in the default English units for their parameters.\r\n2. SI.   Specifies the SI unit system.  Location level values will be in the default SI units for their parameters.\r\n3. Other. Any unit returned in the response to the units URI request that is appropriate for the requested parameters."),
            @OpenApiParam(name="datum", required=false, description="Specifies the elevation datum of the response. This field affects only elevation location levels. Valid values for this field are:\r\n1. NAVD88.  The elevation values will in the specified or default units above the NAVD-88 datum.\r\n2. NGVD29.  The elevation values will be in the specified or default units above the NGVD-29 datum."),
            @OpenApiParam(name="begin", required=false, description="Specifies the start of the time window for data to be included in the response. If this field is not specified, any required time window begins 24 hours prior to the specified or default end time."),
            @OpenApiParam(name="end", required=false, description="Specifies the end of the time window for data to be included in the response. If this field is not specified, any required time window ends at the current time"),
            @OpenApiParam(name="timezone", required=false, description="Specifies the time zone of the values of the begin and end fields (unless otherwise specified), as well as the time zone of any times in the response. If this field is not specified, the default time zone of UTC shall be used."),
            @OpenApiParam(name="format", required=false, description="Specifies the encoding format of the response. Valid values for the format field for this URI are:\r\n1.    tab\r\n2.    csv\r\n3.    xml\r\n4.  wml2 (only if name field is specified)\r\n5.    json (default)")
        },
        responses = {
            @OpenApiResponse(status="200" )
        },
        tags = {"Levels"}
    )
    @Override
    public void getAll(Context ctx)
    {
        getAllRequests.mark();
        try (
            final Timer.Context time_context = getAllRequestsTime.time();
            CwmsDataManager cdm = new CwmsDataManager(ctx);
        ) {
            String format = ctx.queryParamAsClass("format",String.class).getOrDefault("json");
            String names = ctx.queryParam("name");
            String office = ctx.queryParam("office");
            String unit = ctx.queryParam("unit");
            String datum = ctx.queryParam("datum");
            String begin = ctx.queryParam("begin");
            String end = ctx.queryParam("end");
            String timezone = ctx.queryParam("timezone");


            switch(format){
                case "json": {ctx.contentType(Formats.JSON); break;}
                case "tab": {ctx.contentType(Formats.TAB); break;}
                case "csv": {ctx.contentType(Formats.CSV); break;}
                case "xml": {ctx.contentType(Formats.XML); break;}
                case "wml2": {ctx.contentType(Formats.WML2); break;}
                case "png": // fall next
                case "jpg": // fall next
                default: {
                    ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
                }
            }

            String results = cdm.getLocationLevels(format,names,office,unit,datum,begin,end,timezone);
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);
            requestResultSize.update(results.length());
        } catch (SQLException ex) {
            RadarError re = new RadarError("Internal Error");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }


    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String id) {
         ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name="office", required = true, description="Specifies the office in which Location Level will be updated"),
                    @OpenApiParam(name="date", required = true, description="Specifies the effective date of Location Level that will be updated")
            },
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = LocationLevel.class, type = Formats.JSON )
                    },
                    required = true),
            description = "Update CWMS Location Level",
            method = HttpMethod.PATCH,
            path = "/levels",
            tags = {"Levels"}
    )
    @Override
    public void update(Context ctx, String id)
    {
        updateRequest.mark();
        try(final Timer.Context timeContext = updateRequestTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            LocationLevelsDao levelsDao = new LocationLevelsDaoImpl(dsl);
            String office = ctx.queryParam("office");
            String dateString = ctx.queryParam("date");
            ZonedDateTimeAdapter zonedDateTimeAdapter = new ZonedDateTimeAdapter();
            ZonedDateTime unmarshalledDateTime = zonedDateTimeAdapter.unmarshal(dateString);
            ZoneId timezoneId = unmarshalledDateTime.getZone();
            if(timezoneId == null)
            {
                timezoneId = ZoneId.systemDefault();
            }
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            if(contentType == null)
            {
                throw new FormattingException("Format header could not be parsed");
            }
            LocationLevel levelFromBody = deserializeLocationLevel(ctx.body(), contentType.getType(), office);
            //retrieveLocationLevel will throw an error if level does not exist
            LocationLevel existingLocationLevel = levelsDao.retrieveLocationLevel(id, UnitSystem.EN.getValue(), unmarshalledDateTime, office);
            existingLocationLevel = updatedClearedFields(ctx.body(), contentType.getType(), existingLocationLevel);
            //only store (update) if level does exist
            LocationLevel updatedLocationLevel = getUpdatedLocationLevel(existingLocationLevel, levelFromBody);
            updatedLocationLevel.setLevelDate(unmarshalledDateTime);
            if(!updatedLocationLevel.getLocationId().equalsIgnoreCase(existingLocationLevel.getLocationId())) //if name changed then delete location with old name
            {
                levelsDao.renameLocationLevel(id, updatedLocationLevel);
                ctx.status(HttpServletResponse.SC_ACCEPTED).json("Updated and renamed Location Level");
            }
            else
            {
                levelsDao.storeLocationLevel(updatedLocationLevel, timezoneId);
                ctx.status(HttpServletResponse.SC_ACCEPTED).json("Updated Location Level");
            }
        }
        catch(Exception ex)
        {
            RadarError re = new RadarError("Failed to process request: " + ex.getLocalizedMessage());
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private LocationLevel getUpdatedLocationLevel(LocationLevel existingLevel, LocationLevel updatedLevel)
    {
        LocationLevel retVal = new LocationLevel(existingLevel);
        retVal.setSeasonalTimeSeriesId(updatedLevel.getSeasonalTimeSeriesId() == null ? retVal.getSeasonalTimeSeriesId() : updatedLevel.getSeasonalTimeSeriesId());
        retVal.setSeasonalValues(updatedLevel.getSeasonalValues() == null ? retVal.getSeasonalValues() : updatedLevel.getSeasonalValues());
        retVal.setSpecifiedLevelId(updatedLevel.getSpecifiedLevelId() == null ? retVal.getSpecifiedLevelId() : updatedLevel.getSpecifiedLevelId());
        retVal.setParameterTypeId(updatedLevel.getParameterTypeId() == null ? retVal.getParameterTypeId() : updatedLevel.getParameterTypeId());
        retVal.setParameterId(updatedLevel.getParameterId() == null ? retVal.getParameterId() : updatedLevel.getParameterId());
        retVal.setSiParameterUnitsConstantValue(updatedLevel.getSiParameterUnitsConstantValue() == null ? retVal.getSiParameterUnitsConstantValue() : updatedLevel.getSiParameterUnitsConstantValue());
        retVal.setLevelUnitsId(updatedLevel.getLevelUnitsId() == null ? retVal.getLevelUnitsId() : updatedLevel.getLevelUnitsId());
        retVal.setLevelDate(updatedLevel.getLevelDate() == null ? retVal.getLevelDate() : updatedLevel.getLevelDate());
        retVal.setLevelComment(updatedLevel.getLevelComment() == null ? retVal.getLevelComment() : updatedLevel.getLevelComment());
        retVal.setIntervalOrigin(updatedLevel.getIntervalOrigin() == null ? retVal.getIntervalOrigin() : updatedLevel.getIntervalOrigin());
        retVal.setIntervalMonths(updatedLevel.getIntervalMonths() == null ? retVal.getIntervalMonths() : updatedLevel.getIntervalMonths());
        retVal.setInterpolateString(updatedLevel.getInterpolateString() == null ? retVal.getInterpolateString() : updatedLevel.getInterpolateString());
        retVal.setDurationId(updatedLevel.getDurationId() == null ? retVal.getDurationId() : updatedLevel.getDurationId());
        retVal.setAttributeValue(updatedLevel.getAttributeValue() == null ? retVal.getAttributeValue() : updatedLevel.getAttributeValue());
        retVal.setAttributeUnitsId(updatedLevel.getAttributeUnitsId() == null ? retVal.getAttributeUnitsId() : updatedLevel.getAttributeUnitsId());
        retVal.setAttributeParameterTypeId(updatedLevel.getAttributeParameterTypeId() == null ? retVal.getAttributeParameterTypeId() : updatedLevel.getAttributeParameterTypeId());
        retVal.setAttributeParameterId(updatedLevel.getAttributeParameterId() == null ? retVal.getAttributeParameterId() : updatedLevel.getAttributeParameterId());
        retVal.setAttributeDurationId(updatedLevel.getAttributeDurationId() == null ? retVal.getAttributeDurationId() : updatedLevel.getAttributeDurationId());
        retVal.setAttributeComment(updatedLevel.getAttributeComment() == null ? retVal.getAttributeComment() : updatedLevel.getAttributeComment());
        retVal.setLocationId(updatedLevel.getLocationId() == null ? retVal.getLocationId() : updatedLevel.getLocationId());
        retVal.setOfficeId(updatedLevel.getOfficeId() == null ? retVal.getOfficeId() : updatedLevel.getOfficeId());
        if(retVal.getIntervalMonths() != null && retVal.getIntervalMonths() > 0)
        {
            retVal.setIntervalMinutes(null);
        }
        else if(retVal.getIntervalMinutes() != null && retVal.getIntervalMinutes() > 0)
        {
            retVal.setIntervalMonths(null);
        }
        if(retVal.getAttributeValue() == null)
        {
            retVal.setAttributeUnitsId(null);
        }
        if(!retVal.getSeasonalValues().isEmpty())
        {
            retVal.setSiParameterUnitsConstantValue(null);
            retVal.setSeasonalTimeSeriesId(null);
        }
        else if(retVal.getSeasonalTimeSeriesId() != null && !retVal.getSeasonalTimeSeriesId().isEmpty())
        {
            retVal.setSiParameterUnitsConstantValue(null);
            retVal.setSeasonalValues(null);
        }
        return retVal;
    }

    private LocationLevel deserializeLocationLevel(String body, String format, String office) throws IOException
    {
        ObjectMapper om = getObjectMapperForFormat(format);
        LocationLevel retVal;
        try
        {
            retVal = om.readValue(body, LocationLevel.class);
            retVal.setOfficeId(office);
        }
        catch(Exception e)
        {
            logger.log(Level.SEVERE, "Failed to deserialize level", e);
            throw new IOException("Failed to deserialize level");
        }
        return retVal;
    }

    private static ObjectMapper getObjectMapperForFormat(String format)
    {
        ObjectMapper om = new ObjectMapper();
        if((Formats.XML).equals(format))
        {
            om = new XmlMapper();
        }
        om.registerModule(new JavaTimeModule());
        return om;
    }

    private LocationLevel updatedClearedFields(String body, String format, LocationLevel existingLocation) throws IOException
    {
        ObjectMapper om = getObjectMapperForFormat(format);
        JsonNode root = om.readTree(body);
        JavaType javaType = om.getTypeFactory().constructType(LocationLevel.class);
        BeanDescription beanDescription = om.getSerializationConfig().introspect(javaType);
        List<BeanPropertyDefinition> properties = beanDescription.findProperties();
        LocationLevel retVal = new LocationLevel(existingLocation);
        try
        {
            for (BeanPropertyDefinition propertyDefinition : properties) {
                String propertyName = propertyDefinition.getName();
                JsonNode propertyValue = root.findValue(propertyName);
                if (propertyValue != null && "".equals(propertyValue.textValue())) {
                    retVal.setProperty(propertyName, propertyValue);
                }
            }
        }
        catch (NullPointerException e) //gets thrown if required field is null
        {
            throw new IOException(e.getMessage());
        }
        return retVal;
    }

}
