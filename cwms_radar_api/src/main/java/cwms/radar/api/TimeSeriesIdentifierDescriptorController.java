package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.TimeSeriesIdentifierDescriptorDao;
import cwms.radar.data.dto.TimeSeriesIdentifier;
import cwms.radar.data.dto.TimeSeriesIdentifierDescriptor;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.helpers.DateUtils;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;

public class TimeSeriesIdentifierDescriptorController implements CrudHandler {
    public static final Logger logger =
            Logger.getLogger(TimeSeriesIdentifierDescriptorController.class.getName());
    public static final String TAG = "TimeSeries Identifier-Beta";
    public static final String OFFICE = "office";
    public static final String TIMESERIES_ID_REGEX = "timeseries-id-regex";
    public static final String TIMEZONE = "timezone";
    public static final String START_TIME = "start-time";
    public static final String END_TIME = "end-time";
    public static final String VERSION_DATE = "version-date";
    public static final String START_TIME_INCLUSIVE = "start-time-inclusive";
    public static final String END_TIME_INCLUSIVE = "end-time-inclusive";
    public static final String MAX_VERSION = "max-version";
    public static final String OVERRIDE_PROTECTION = "override-protection";
    public static final String TS_ITEM_MASK = "ts_item_mask";
    public static final String TIMESERIES_ID = "timeseries-id";
    public static final String SNAP_FORWARD = "snap-forward";
    public static final String SNAP_BACKWARD = "snap-backward";
    public static final String ACTIVE_FLAG = "active-flag";

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

            String result = null;  /// Formats.format(contentType, ids, TimeSeriesIdentifier.class);

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
                String result = null;  /// Formats.format(contentType, grp.get());

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
    public void create(Context ctx) {
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
                            + "If specified a rename operation will be performed and " +
                            SNAP_FORWARD + ", " + SNAP_BACKWARD + ", and " + ACTIVE_FLAG + " must not be provided"),
                    @OpenApiParam(name = "utc-offset", description = "The offset into the utc data interval in minutes.  "
                            + "If specified and a new timeseries-id is also specified both will be passed to a rename operation.  May also be passed to and update operation."
                            ),
                    @OpenApiParam(name = SNAP_FORWARD, description = "The new snap forward tolerance in minutes. This specifies how many minutes before the expected data time that data will be considered to be on time."),
                    @OpenApiParam(name = SNAP_BACKWARD, description = "The new snap backward tolerance in minutes. This specifies how many minutes after the expected data time that data will be considered to be on time."),
                    @OpenApiParam(name = ACTIVE_FLAG, description = "'True' or 'true' if the time series is active")
            }
    )
    @Override
    public void update(Context ctx, @NotNull String timeseriesId) {

        String office = ctx.queryParam(OFFICE);
        String newTimeseriesId = ctx.queryParam(TIMESERIES_ID);
        Long utcOffset = ctx.queryParamAsClass("utc-offset", Long.class).getOrDefault(null);

        List<String> updateKeys = Arrays.asList(SNAP_FORWARD, SNAP_BACKWARD, ACTIVE_FLAG);

        Map<String, List<String>> paramMap = ctx.queryParamMap();
        List<String> foundUpdateKeys = updateKeys.stream()
                .filter(paramMap::containsKey)
                .collect(Collectors.toList());

        if(!foundUpdateKeys.isEmpty() && newTimeseriesId != null) {
            throw new IllegalArgumentException("Cannot specify a new timeseries-id and any of the following update parameters: " + foundUpdateKeys);
        }
        

        try (final Timer.Context ignored = markAndTime("update");
             DSLContext dsl = getDslContext(ctx)) {
            TimeSeriesIdentifierDescriptorDao dao = new TimeSeriesIdentifierDescriptorDao(dsl);

            if(foundUpdateKeys.isEmpty()){
                // basic rename.
                dao.rename(office, timeseriesId, newTimeseriesId, utcOffset);
            } else {
                Long forward = ctx.queryParamAsClass(SNAP_FORWARD, Long.class).getOrDefault(null);
                Long backward = ctx.queryParamAsClass(SNAP_BACKWARD, Long.class).getOrDefault(null);

                boolean active = ctx.queryParamAsClass(ACTIVE_FLAG, Boolean.class).getOrDefault(true);

                dao.update(office, timeseriesId, utcOffset, forward, backward, active);
            }





        }



    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = TIMESERIES_ID, required = true, description = "The timeseries-id of the timeseries to be deleted. "
                            + "If '" + OFFICE +"' is the only additional query parameter specified a simple delete will be performed."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the office of the timeseries to be deleted."),
                    @OpenApiParam(name = START_TIME, description = "The start of the time window in the specified or default time zone"),
                    @OpenApiParam(name = END_TIME, description = "The end of the time window in the specified or default time zone"),
                    @OpenApiParam(name = TIMEZONE, description = "Specifies the time zone of the values of the begin and end fields "
                            + "(unless otherwise specified), If this field is not specified, the default time zone of UTC shall be used."),
                    @OpenApiParam(name = VERSION_DATE, description = "The version date/time of the time series in the specified or default time zone. If NULL, the earliest or latest version date will be used depending on p_max_version."),
                    @OpenApiParam(name = START_TIME_INCLUSIVE, description = "A flag specifying whether any data at the start time should be deleted ('True') or only data <b><em>after</em></b> the start time ('False')"),
                    @OpenApiParam(name = END_TIME_INCLUSIVE, description = "A flag ('True'/'False') specifying whether any data at the end time should be deleted ('True') or only data <b><em>before</em></b> the end time ('False')"),
                    @OpenApiParam(name = MAX_VERSION, description = "A flag ('True'/'False') specifying whether to use the earliest ('False') or latest ('True') version date for each time if p_version_date is NULL."),
                    @OpenApiParam(name = TS_ITEM_MASK, description = "A cookie specifying what time series items to purge."),
                    @OpenApiParam(name = OVERRIDE_PROTECTION, description = "A flag ('True'/'False'/'E') specifying whether to delete protected data, Set to 'E' to raise an exception if protected values are encountered."),
            },

            description = "Deletes requested timeseries identifier"
           )

    @Override
    public void delete(Context ctx, String timeseriesId) {

        try (final Timer.Context ignored = markAndTime("delete");
             DSLContext dsl = getDslContext(ctx)) {
            TimeSeriesIdentifierDescriptorDao dao = new TimeSeriesIdentifierDescriptorDao(dsl);

            String office = ctx.queryParam(OFFICE);

            List<String> expandedDeleteKeys = Arrays.asList(new String[]{START_TIME,
                    END_TIME, VERSION_DATE, START_TIME_INCLUSIVE, END_TIME_INCLUSIVE, MAX_VERSION,
                    TS_ITEM_MASK, OVERRIDE_PROTECTION});

            Map<String, String> paramMap = ctx.pathParamMap();

            boolean useExpanded = expandedDeleteKeys.stream().anyMatch((key) -> paramMap.containsKey(key));

            if (useExpanded) {
                String timezone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");

                Date startTimeDate = getDate(timezone, ctx.queryParam(START_TIME));
                Date endTimeDate = getDate(timezone, ctx.queryParam(END_TIME));
                Date versionDate = getDate(timezone, ctx.queryParam(VERSION_DATE));

                // FYI queryParamAsClass with Boolean.class returns a case insensitive comparison to "true".
                boolean startTimeInclusive = ctx.queryParamAsClass(START_TIME_INCLUSIVE, Boolean.class).get();
                boolean endTimeInclusive = ctx.queryParamAsClass(END_TIME_INCLUSIVE, Boolean.class).get();

                Boolean maxVersion = ctx.queryParamAsClass(MAX_VERSION, Boolean.class).get();
                Integer tsItemMask = ctx.queryParamAsClass(TS_ITEM_MASK, Integer.class).get();
                TimeSeriesIdentifierDescriptorDao.OverrideProtection op =
                        ctx.queryParamAsClass(OVERRIDE_PROTECTION,
                                TimeSeriesIdentifierDescriptorDao.OverrideProtection.class).get();

                TimeSeriesIdentifierDescriptorDao.DeleteOptions options = new TimeSeriesIdentifierDescriptorDao.DeleteOptions.Builder()
                        .withStartTime(startTimeDate)
                        .withEndTime(endTimeDate)
                        .withVersionDate(versionDate)
                        .withStartTimeInclusive(startTimeInclusive)
                        .withEndTimeInclusive(endTimeInclusive)
                        .withMaxVersion(maxVersion)
                        .withTsItemMask(tsItemMask)
//                        .withDateTimesSet(dateTimesSet)
                        .withOverrideProtection(op.toString())
                        .build();

                dao.delete(office, timeseriesId, options);
            } else {
                dao.deleteAll(office, timeseriesId);
            }

        }


    }

    @Nullable
    private static Date getDate(String timezone, String startTimeStr) {
        Date startTimeDate = null;
        if (startTimeStr != null && startTimeStr.isEmpty()) {
            ZonedDateTime startTimeZDT = DateUtils.parseUserDate(startTimeStr, timezone);
            startTimeDate = Date.from(startTimeZDT.toInstant());
        }
        return startTimeDate;
    }
}
