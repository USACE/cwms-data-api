package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.api.Controllers.CATEGORY_ID;
import static cwms.radar.api.Controllers.GET_ALL;
import static cwms.radar.api.Controllers.GET_ONE;
import static cwms.radar.api.Controllers.GROUP_ID;
import static cwms.radar.api.Controllers.INCLUDE_ASSIGNED;
import static cwms.radar.api.Controllers.INCLUDE_ASSIGNED2;
import static cwms.radar.api.Controllers.OFFICE;
import static cwms.radar.api.Controllers.RESULTS;
import static cwms.radar.api.Controllers.SIZE;
import static cwms.radar.api.Controllers.queryParamAsClass;
import static cwms.radar.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.LocationGroupDao;
import cwms.radar.data.dto.LocationGroup;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.csv.CsvV1LocationGroup;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.geojson.FeatureCollection;
import org.jooq.DSLContext;

public class LocationGroupController implements CrudHandler {
    public static final Logger logger = Logger.getLogger(LocationGroupController.class.getName());

    public static final String TAG = "Location Groups-Beta";
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public LocationGroupController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the "
                    + "location group(s) whose data is to be included in the response. If this "
                    + "field is not specified, matching location groups information from all "
                    + "offices shall be returned."),
            @OpenApiParam(name = INCLUDE_ASSIGNED, type = Boolean.class, description = "Include"
                    + " the assigned locations in the returned location groups. (default: false)"),
            @OpenApiParam(name = INCLUDE_ASSIGNED2, deprecated = true, type = Boolean.class,
                    description = "Deprecated. Use include-assigned instead."),},
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(isArray = true, from = LocationGroup.class,
                                            type = Formats.JSON),
                                    @OpenApiContent(isArray = true, from =
                                            CsvV1LocationGroup.class, type = Formats.CSV)
                            }
                    )},
            description = "Returns CWMS Location Groups Data", tags = {TAG})
    @Override
    public void getAll(Context ctx) {

        try (final Timer.Context timeContext = markAndTime(GET_ALL);
             DSLContext dsl = getDslContext(ctx)
        ) {
            LocationGroupDao cdm = new LocationGroupDao(dsl);

            String office = ctx.queryParam(OFFICE);

            boolean includeAssigned = queryParamAsClass(ctx, new String[]{INCLUDE_ASSIGNED, INCLUDE_ASSIGNED2},
                    Boolean.class, false, metrics, name(LocationGroupController.class.getName(),
                            GET_ALL));

            List<LocationGroup> grps = cdm.getLocationGroups(office, includeAssigned);

            if (!grps.isEmpty()) {
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");

                String result = Formats.format(contentType, grps, LocationGroup.class);

                ctx.result(result);
                ctx.contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            } else {
                RadarError re = new RadarError("No location groups for office provided");
                logger.info(() ->
                        new StringBuilder()
                                .append(re).append(System.lineSeparator())
                                .append("for request ").append(ctx.fullUrl())
                                .toString()
                );
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }

        }

    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = GROUP_ID, required = true, description = "Specifies "
                            + "the location_group whose data is to be included in the response")
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the location group whose data is to be included "
                            + "in the response."),
                    @OpenApiParam(name = CATEGORY_ID, required = true, description = "Specifies"
                            + " the category containing the location group whose data is to be "
                            + "included in the response."),
            },
            responses = {@OpenApiResponse(status = "200",
                    content = {
                            @OpenApiContent(from = LocationGroup.class, type = Formats.JSON),
                            @OpenApiContent(from = CsvV1LocationGroup.class, type = Formats.CSV),
                            @OpenApiContent(type = Formats.GEOJSON)
                    }

            )},
            description = "Retrieves requested Location Group", tags = {TAG})
    @Override
    public void getOne(Context ctx, String groupId) {

        try (final Timer.Context timeContext = markAndTime(GET_ONE);
             DSLContext dsl = getDslContext(ctx)
        ) {
            LocationGroupDao cdm = new LocationGroupDao(dsl);
            String office = ctx.queryParam(OFFICE);
            String categoryId = ctx.queryParam(CATEGORY_ID);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");

            String result;
            if (Formats.GEOJSON.equals(contentType.getType())) {
                FeatureCollection fc = cdm.buildFeatureCollectionForLocationGroup(office,
                        categoryId, groupId, "EN");
                ObjectMapper mapper = ctx.appAttribute("ObjectMapper");
                result = mapper.writeValueAsString(fc);
            } else {
                Optional<LocationGroup> grp = cdm.getLocationGroup(office, categoryId, groupId);
                if (grp.isPresent()) {
                    result = Formats.format(contentType, grp.get());
                } else {
                    RadarError re = new RadarError("Unable to find location group based on "
                            + "parameters given");
                    logger.info(() ->
                            new StringBuilder()
                                    .append(re).append(System.lineSeparator())
                                    .append("for request ").append(ctx.fullUrl())
                                    .toString()
                    );
                    ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
                    return;
                }

            }
            ctx.result(result);
            ctx.contentType(contentType.toString());

            requestResultSize.update(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        } catch (JsonProcessingException e) {
            RadarError re = new RadarError("Failed to process request");
            logger.log(Level.SEVERE, re.toString(), e);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String groupId) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String groupId) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }
}
