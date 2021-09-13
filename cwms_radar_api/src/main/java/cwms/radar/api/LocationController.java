package cwms.radar.api;

import java.io.IOException;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.Null;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.radar.api.enums.Nation;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.LocationsDao;
import cwms.radar.data.dao.LocationsDaoImpl;
import cwms.radar.data.dto.Location;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.openapi.annotations.*;
import org.geojson.FeatureCollection;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.JooqDao.getDslContext;


/**
 *
 *
 */
public class LocationController implements CrudHandler {
    public static final Logger logger = Logger.getLogger(LocationController.class.getName());
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

    public LocationController(MetricRegistry metrics){
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
            @OpenApiParam( name="names", description = "Specifies the name(s) of the location(s) whose data is to be included in the response"),
            @OpenApiParam(name="office", description="Specifies the owning office of the location level(s) whose data is to be included in the response. If this field is not specified, matching location level information from all offices shall be returned."),
            @OpenApiParam(name="unit",   description="Specifies the unit or unit system of the response. Valid values for the unit field are:\r\n 1. EN.   Specifies English unit system.  Location level values will be in the default English units for their parameters.\r\n2. SI.   Specifies the SI unit system.  Location level values will be in the default SI units for their parameters.\r\n3. Other. Any unit returned in the response to the units URI request that is appropriate for the requested parameters."),
            @OpenApiParam(name="datum",  description="Specifies the elevation datum of the response. This field affects only elevation location levels. Valid values for this field are:\r\n1. NAVD88.  The elevation values will in the specified or default units above the NAVD-88 datum.\r\n2. NGVD29.  The elevation values will be in the specified or default units above the NGVD-29 datum."),
            @OpenApiParam(name="format", description="Specifies the encoding format of the response. Valid values for the format field for this URI are:\r\n1.    tab\r\n2.    csv\r\n3.    xml\r\n4.  wml2 (only if name field is specified)\r\n5.    json (default)\n" + "6.    geojson")
        },
        responses = {
            @OpenApiResponse( status="200",
                    content = {
                            @OpenApiContent(type = Formats.JSON ),
                            @OpenApiContent(type = Formats.TAB ),
                            @OpenApiContent(type = Formats.CSV ),
                            @OpenApiContent(type = Formats.XML ),
                            @OpenApiContent(type = Formats.WML2),
                            @OpenApiContent(type = Formats.GEOJSON )
                    })
        },
        description = "Returns CWMS Location Data",
        tags = {"Locations"}
    )
    @Override
    public void getAll(Context ctx)
    {
        getAllRequests.mark();
        try(final Timer.Context timeContext = getAllRequestsTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            LocationsDao locationsDao = new LocationsDaoImpl(dsl);

            String names = ctx.queryParam("names");
            String units = ctx.queryParam("unit");
            String datum = ctx.queryParam("datum");
            String office = ctx.queryParam("office");

            String formatParm = ctx.queryParam("format", "");
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm);
            ctx.contentType(contentType.toString());

            final String results;
            if(contentType.getType().equals(Formats.GEOJSON))
            {
                logger.fine("units:" + units);
                FeatureCollection collection = locationsDao.buildFeatureCollection(names, units, office);
                ObjectMapper mapper = JavalinJackson.getObjectMapper();
                results = mapper.writeValueAsString(collection);
            }
            else
            {
                String format = getFormatFromContent(contentType);
                results = locationsDao.getLocations(names, format, units, datum, office);
            }

            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);
            requestResultSize.update(results.length());
        }
        catch( JsonProcessingException ex)
        {
            RadarError re = new RadarError("failed to process request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private String getFormatFromContent(ContentType contentType)
    {
        String format = "json";
        if(contentType != null)
        {
            String type = contentType.getType();
            // Seems weird to map back to format from contentType but we really want them to agree.
            // What if format wasn't provided but an accept header for csv was?
            // I think we would want to pass "csv" to the db procedure.
            Map<String, String> lookup = new LinkedHashMap<>();
            lookup.put(Formats.TAB, "tab");
            lookup.put(Formats.CSV, "csv");
            lookup.put(Formats.XML, "xml");
            lookup.put(Formats.WML2, "wml2");
            lookup.put(Formats.JSON, "json");
            if(lookup.containsKey(type))
            {
                format = lookup.get(type);
            }
        }
        return format;
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String name) {
        getOneRequest.mark();
        try (
            final Timer.Context timeContext = getOneRequestTime.time()
            ){
                ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
        }
    }

    @OpenApi(
        requestBody = @OpenApiRequestBody(
                content = {
                          @OpenApiContent(from = Location.class, type = Formats.JSON ),

//                        @OpenApiContent(from = Location.class, type = Formats.TAB ),
//                        @OpenApiContent(from = Location.class, type = Formats.CSV ),
//                        @OpenApiContent(from = Location.class, type = Formats.XML ),
//                        @OpenApiContent(from = Location.class, type = Formats.WML2),
//                        @OpenApiContent(from = Location.class, type = Formats.GEOJSON )
                },
                required = true),
        description = "Create new CWMS Location",
        method = HttpMethod.POST,
        path = "/locations",
        tags = {"Locations"}
    )
    @Override
    public void create(Context ctx)
    {
        createRequest.mark();
        try(final Timer.Context timeContext = createRequestTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            LocationsDao locationsDao = new LocationsDaoImpl(dsl);
            String acceptHeader = ctx.header(Header.ACCEPT);
            String formatHeader = (acceptHeader != null) ? acceptHeader : Formats.JSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            if(contentType == null)
            {
                throw new FormattingException("Format header could not be parsed");
            }
            if(contentType.getType().equals(Formats.JSON))
            {
                ObjectMapper om = new ObjectMapper();
                om.registerModule(new JavaTimeModule());
                Location location = om.readValue(ctx.body(), Location.class);
                locationsDao.storeLocation(location);
                ctx.status(HttpServletResponse.SC_CREATED).json(location.getName() + " Created");
            }
        }
        catch(IOException ex)
        {
            RadarError re = new RadarError("failed to process request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                          @OpenApiContent(from = Location.class, type = Formats.JSON )
//                        @OpenApiContent(from = Location.class, type = Formats.TAB ),
//                        @OpenApiContent(from = Location.class, type = Formats.CSV ),
//                        @OpenApiContent(from = Location.class, type = Formats.XML ),
//                        @OpenApiContent(from = Location.class, type = Formats.WML2),
//                        @OpenApiContent(from = Location.class, type = Formats.GEOJSON )
                    },
                    required = true),
            description = "Update CWMS Location",
            method = HttpMethod.PATCH,
            path = "/locations",
            tags = {"Locations"}
    )
    @Override
    public void update(Context ctx, @NotNull String locationId)
    {
        updateRequest.mark();
        String deserializationErrorMsg = null;
        try(final Timer.Context timeContext = updateRequestTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            LocationsDao locationsDao = new LocationsDaoImpl(dsl);
            String acceptHeader = ctx.header(Header.ACCEPT);
            String formatHeader = (acceptHeader != null) ? acceptHeader : Formats.JSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            if(contentType == null)
            {
                throw new FormattingException("Format header could not be parsed");
            }
            if(contentType.getType().equals(Formats.JSON))
            {
                ObjectMapper om = new ObjectMapper();
                om.registerModule(new JavaTimeModule());
                Location locationFromBody = deserializeJSONLocation(ctx.body());
                //getLocation will throw an error if location does not exist
                Location existingLocation = locationsDao.getLocation(locationId, UnitSystem.EN.getValue(), locationFromBody.getOfficeId());
                //only store (update) if location does exist
                Location updatedLocation = getUpdatedLocation(existingLocation, locationFromBody);
                if(!updatedLocation.getName().equalsIgnoreCase(existingLocation.getName())) //if name changed then delete location with old name
                {
                    locationsDao.renameLocation(locationId, updatedLocation);
                    ctx.status(HttpServletResponse.SC_ACCEPTED).json("Updated and renamed Location");
                }
                else
                {
                    locationsDao.storeLocation(updatedLocation);
                    ctx.status(HttpServletResponse.SC_ACCEPTED).json("Updated Location");
                }

            }
        }
        catch(IOException ex)
        {
            RadarError re = new RadarError("Failed to process request: " + ex.getLocalizedMessage());
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = "office", description = "Specifies the owning office of the location whose data is to be deleted. If this field is not specified, matching location information will be deleted from all offices.")
            },
            description = "Delete CWMS Location",
            method = HttpMethod.DELETE,
            path = "/locations",
            tags = {"Locations"}
    )
    @Override
    public void delete(Context ctx, @NotNull String locationId)
    {
        deleteRequest.mark();
        try(final Timer.Context timeContext = deleteRequestTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            String office = ctx.queryParam("office");
            LocationsDao locationsDao = new LocationsDaoImpl(dsl);
            locationsDao.deleteLocation(locationId, office);
            ctx.status(HttpServletResponse.SC_ACCEPTED).json(locationId + " Deleted");
        }
        catch(IOException ex)
        {
            RadarError re = new RadarError("Failed to delete location");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private Location deserializeJSONLocation(String body) throws IOException
    {
        ObjectMapper om = new ObjectMapper();
        om.registerModule(new JavaTimeModule());
        Location retVal;
        try
        {
            retVal = om.readValue(body, Location.class);
        }
        catch (MismatchedInputException ex)
        {
            String error = ex.getLocalizedMessage();
            int firstQuoteIndex = error.indexOf('\'');
            int secondQuoteIndex = error.indexOf('\'', firstQuoteIndex +1);
            String missingField = error.substring(firstQuoteIndex + 1, secondQuoteIndex);
            throw new IOException("Missing required field: " + missingField);
        }
        return retVal;
    }


    private Location getUpdatedLocation(Location existingLocation, Location updatedLocation)
    {
        String updatedName = updatedLocation.getName() == null ? existingLocation.getName() : updatedLocation.getName();
        Double updatedLatitude = updatedLocation.getLatitude() == null ? existingLocation.getLatitude() : updatedLocation.getLatitude();
        Double updatedLongitude = updatedLocation.getLongitude() == null ? existingLocation.getLongitude() : updatedLocation.getLongitude();
        Boolean updatedIsActive = updatedLocation.active() == null ? existingLocation.active() : updatedLocation.active();
        String updatedPublicName = updatedLocation.getPublicName() == null ? existingLocation.getPublicName() : updatedLocation.getPublicName();
        String updatedLongName = updatedLocation.getLongName() == null ? existingLocation.getLongName() : updatedLocation.getLongName();
        String updatedDescription = updatedLocation.getDescription() == null ? existingLocation.getDescription() : updatedLocation.getDescription();
        String updatedTimeZoneId = updatedLocation.getTimezoneId() == null ? existingLocation.getTimezoneId() : updatedLocation.getTimezoneId();
        String updatedLocationType = updatedLocation.getLocationType() == null ? existingLocation.getLocationType() : updatedLocation.getLocationType();
        String updatedLocationKind = updatedLocation.getLocationKind() == null ? existingLocation.getLocationKind() : updatedLocation.getLocationKind();
        Nation updatedNation = updatedLocation.getNation() == null ? existingLocation.getNation() : updatedLocation.getNation();
        String updatedStateInitial = updatedLocation.getStateInitial() == null ? existingLocation.getStateInitial() : updatedLocation.getStateInitial();
        String updatedCountyName = updatedLocation.getCountyName() == null ? existingLocation.getCountyName() : updatedLocation.getCountyName();
        String updatedNearestCity = updatedLocation.getNearestCity() == null ? existingLocation.getNearestCity() : updatedLocation.getNearestCity();
        String updatedHorizontalDatum = updatedLocation.getHorizontalDatum() == null ? existingLocation.getHorizontalDatum() : updatedLocation.getHorizontalDatum();
        Double updatedPublishedLongitude = updatedLocation.getPublishedLongitude() == null ? existingLocation.getPublishedLongitude() : updatedLocation.getPublishedLongitude();
        Double updatedPublishedLatitude = updatedLocation.getPublishedLatitude() == null ? existingLocation.getPublishedLatitude() : updatedLocation.getPublishedLatitude();
        String updatedVerticalDatum = updatedLocation.getVerticalDatum() == null ? existingLocation.getVerticalDatum() : updatedLocation.getVerticalDatum();
        Double updatedElevation = updatedLocation.getElevation() == null ? existingLocation.getElevation() : updatedLocation.getElevation();
        String updatedMapLabel = updatedLocation.getMapLabel() == null ? existingLocation.getMapLabel() : updatedLocation.getMapLabel();
        String updatedBoundingOfficeId = updatedLocation.getBoundingOfficeId() == null ? existingLocation.getBoundingOfficeId() : updatedLocation.getBoundingOfficeId();
        String updatedOfficeId = updatedLocation.getOfficeId() == null ? existingLocation.getOfficeId() : updatedLocation.getOfficeId();
        return new Location.Builder(updatedName, updatedLocationKind, ZoneId.of(updatedTimeZoneId), updatedLatitude, updatedLongitude, updatedHorizontalDatum, updatedOfficeId)
                .withActive(updatedIsActive)
                .withPublicName(updatedPublicName)
                .withLongName(updatedLongName)
                .withDescription(updatedDescription)
                .withLocationType(updatedLocationType)
                .withNation(updatedNation)
                .withStateInitial(updatedStateInitial)
                .withCountyName(updatedCountyName)
                .withNearestCity(updatedNearestCity)
                .withPublishedLongitude(updatedPublishedLongitude)
                .withPublishedLatitude(updatedPublishedLatitude)
                .withVerticalDatum(updatedVerticalDatum)
                .withElevation(updatedElevation)
                .withMapLabel(updatedMapLabel)
                .withBoundingOfficeId(updatedBoundingOfficeId)
                .build();
    }

}
