package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.JooqDao;
import cwms.radar.data.dao.TimeSeriesDao;
import cwms.radar.data.dao.TimeSeriesDaoImpl;
import cwms.radar.data.dao.TimeSeriesIdentifierDescriptorDao;
import cwms.radar.data.dto.TimeSeriesIdentifier;
import cwms.radar.data.dto.TimeSeriesIdentifierDescriptor;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

public class TimeSeriesIdentifierDescriptorController implements CrudHandler {
    public static final Logger logger =
            Logger.getLogger(TimeSeriesIdentifierDescriptorController.class.getName());
    public static final String TAG = "TimeSeries Identifier-Beta";
    public static final String OFFICE = "office";
    public static final String TIMESERIES_ID_REGEX = "timeseries-id-regex";
    public static final String TIMESERIES_ID = "timeseries-id";
    public static final String SNAP_FORWARD = "snap-forward";
    public static final String SNAP_BACKWARD = "snap-backward";
    public static final String ACTIVE = "active";
    public static final String UTC_OFFSET = "utc-offset";
    public static final String METHOD = "method";

    private final MetricRegistry metrics;


    private final Histogram requestResultSize;

    public TimeSeriesIdentifierDescriptorController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, "results", "size")));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    @NotNull
    protected TimeSeriesDao getTimeSeriesDao(DSLContext dsl) {
        return new TimeSeriesDaoImpl(dsl);
    }

    @OpenApi(queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the "
                    + "timeseries identifier(s) whose data is to be included in the response. If "
                    + "this field is not specified, matching timeseries identifier information from"
                    + " all offices shall be returned."),
            @OpenApiParam(name = TIMESERIES_ID_REGEX, description = "A case insensitive RegExp "
                    + "that will be applied to the timeseries-id field. If this field is "
                    + "not specified the results will not be constrained by timeseries-id."),
            },
            responses = {@OpenApiResponse(status = "200",
                    content = {@OpenApiContent(isArray = true, from = TimeSeriesIdentifier.class,
                            type = Formats.JSON)
                    }),
                    @OpenApiResponse(status = "404", description = "Based on the combination of "
                            + "inputs provided the categories were not found."),
                    @OpenApiResponse(status = "501", description = "request format is not "
                            + "implemented")}, description = "Returns CWMS timeseries identifier "
            + "Data", tags = {TAG})
    @Override
    public void getAll(Context ctx) {

        try (final Timer.Context ignored = markAndTime("getAll");
             DSLContext dsl = getDslContext(ctx)) {
            TimeSeriesIdentifierDescriptorDao dao = new TimeSeriesIdentifierDescriptorDao(dsl);
            String office = ctx.queryParam(OFFICE);
            String idRegex = ctx.queryParam(TIMESERIES_ID_REGEX);

            List<TimeSeriesIdentifierDescriptor> ids =
                    dao.getTimeSeriesIdentifiers(office,  idRegex);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

            String result = null;  ///TODO implement this: Formats.format(contentType, ids, TimeSeriesIdentifier.class);

            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        }

    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = TIMESERIES_ID, required = true, description = "Specifies"
                            + " the identifier of the timeseries to be included in the response."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the timeseries identifier to be "
                            + "included in the response."),
            },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(from = TimeSeriesIdentifier.class, type =
                                            Formats.JSON)
                            }
                    ),
                    @OpenApiResponse(status = "404", description = "Based on the combination of "
                            + "inputs provided the timeseries identifier was not found."),
                    @OpenApiResponse(status = "501", description = "request format is not "
                            + "implemented")},
            description = "Retrieves requested timeseries identifier", tags = {TAG})
    @Override
    public void getOne(Context ctx, @NotNull String timeseriesId) {

        try (final Timer.Context ignored = markAndTime("getOne");
             DSLContext dsl = getDslContext(ctx)) {
            TimeSeriesIdentifierDescriptorDao dao = new TimeSeriesIdentifierDescriptorDao(dsl);
            String office = ctx.queryParam(OFFICE);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

            Optional<TimeSeriesIdentifierDescriptor> grp = dao.getTimeSeriesIdentifier(office, timeseriesId);
            if (grp.isPresent()) {
                String result = null;  /// TODO implement this: Formats.format(contentType, grp.get());

                ctx.result(result).contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            } else {
                RadarError re = new RadarError("Unable to find identifier based on parameters "
                        + "given");
                logger.info(() -> re + System.lineSeparator() + "for request " + ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void create(@NotNull Context ctx) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = TIMESERIES_ID, description = "The timeseries id"),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the timeseries identifier to be updated"),
                    @OpenApiParam(name = TIMESERIES_ID, description = "A new timeseries-id.  "
                            + "If specified a rename operation will be performed and "
                            + SNAP_FORWARD + ", " + SNAP_BACKWARD + ", and " + ACTIVE + " must not be provided"),
                    @OpenApiParam(name = UTC_OFFSET, description = "The offset into the utc data interval in minutes.  "
                            + "If specified and a new timeseries-id is also specified both will be passed to a "
                            + "rename operation.  May also be passed to update operation."),
                    @OpenApiParam(name = SNAP_FORWARD, description = "The new snap forward tolerance in minutes. This specifies how many minutes before the expected data time that data will be considered to be on time."),
                    @OpenApiParam(name = SNAP_BACKWARD, description = "The new snap backward tolerance in minutes. This specifies how many minutes after the expected data time that data will be considered to be on time."),
                    @OpenApiParam(name = ACTIVE, description = "'True' or 'true' if the time series is active")
            }
    )
    @Override
    public void update(Context ctx, @NotNull String timeseriesId) {

        String office = ctx.queryParam(OFFICE);
        String newTimeseriesId = ctx.queryParam(TIMESERIES_ID);
        Long utcOffset = ctx.queryParamAsClass(UTC_OFFSET, Long.class).getOrDefault(null);

        List<String> updateKeys = Arrays.asList(SNAP_FORWARD, SNAP_BACKWARD, ACTIVE);

        Map<String, List<String>> paramMap = ctx.queryParamMap();
        List<String> foundUpdateKeys = updateKeys.stream()
                .filter(paramMap::containsKey)
                .collect(Collectors.toList());

        if (!foundUpdateKeys.isEmpty() && newTimeseriesId != null) {
            throw new IllegalArgumentException("Cannot specify a new timeseries-id and any of the"
                    + " following update parameters: " + foundUpdateKeys);
        }
        

        try (final Timer.Context ignored = markAndTime("update");
             DSLContext dsl = getDslContext(ctx)) {
            TimeSeriesIdentifierDescriptorDao dao = new TimeSeriesIdentifierDescriptorDao(dsl);

            if (foundUpdateKeys.isEmpty()) {
                // basic rename.
                dao.rename(office, timeseriesId, newTimeseriesId, utcOffset);
            } else {
                Long forward = ctx.queryParamAsClass(SNAP_FORWARD, Long.class).getOrDefault(null);
                Long backward = ctx.queryParamAsClass(SNAP_BACKWARD, Long.class).getOrDefault(null);
                boolean active = ctx.queryParamAsClass(ACTIVE, Boolean.class).getOrDefault(true);

                dao.update(office, timeseriesId, utcOffset, forward, backward, active);
            }

        }

    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = TIMESERIES_ID, required = true, description = "The timeseries-id of the timeseries to be deleted. "),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the timeseries to be deleted."),
                    @OpenApiParam(name = METHOD,  description = "Specifies the delete method used."
                            + "Default: DELETE_ALL",
                            type = TimeSeriesIdentifierDescriptorDao.DeleteMethod.class)
            },
            description = "Deletes requested timeseries identifier"
           )

    @Override
    public void delete(@NotNull Context ctx, @NotNull String timeseriesId) {

        TimeSeriesIdentifierDescriptorDao.DeleteMethod method = ctx.queryParamAsClass(METHOD,
                        TimeSeriesIdentifierDescriptorDao.DeleteMethod.class).allowNullable().get();

        String office = ctx.queryParam(OFFICE);

        try (final Timer.Context ignored = markAndTime("delete");
             DSLContext dsl = getDslContext(ctx)) {
            TimeSeriesIdentifierDescriptorDao dao = new TimeSeriesIdentifierDescriptorDao(dsl);
            dao.delete(office, timeseriesId, method);

            ctx.status(HttpServletResponse.SC_OK);

        } catch (DataAccessException ex) {
            RadarError re = new RadarError("Internal Error");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

    }

}
