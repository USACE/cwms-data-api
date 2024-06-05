/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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
import static cwms.cda.api.Controllers.ACTIVE;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.INTERVAL_OFFSET;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.SNAP_BACKWARD;
import static cwms.cda.api.Controllers.SNAP_FORWARD;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.TIMESERIES_ID;
import static cwms.cda.api.Controllers.TIMESERIES_ID_REGEX;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.TimeSeriesIdentifierDescriptorDao;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptor;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptors;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
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
    public static final Logger logger = Logger.getLogger(TimeSeriesIdentifierDescriptorController.class.getName());
    public static final String TAG = "TimeSeries Identifier";

    private static final int DEFAULT_PAGE_SIZE = 500;

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public TimeSeriesIdentifierDescriptorController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }


    @OpenApi(queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the "
                    + "timeseries identifier(s) whose data is to be included in the response. If "
                    + "this field is not specified, matching timeseries identifier information from"
                    + " all offices shall be returned."),
            @OpenApiParam(name = TIMESERIES_ID_REGEX, description = "A case insensitive RegExp "
                    + "that will be applied to the timeseries-id field. If this field is "
                    + "not specified the results will not be constrained by timeseries-id."),

            @OpenApiParam(name = PAGE,
                    description = "This end point can return a lot of data, this "
                            + "identifies where in the request you are. This is an opaque"
                            + " value, and can be obtained from the 'next-page' value in "
                            + "the response."
            ),
            @OpenApiParam(name = PAGE_SIZE, type = Integer.class,
                    description = "How many entries per page returned. "
                            + "Default " + DEFAULT_PAGE_SIZE + "."
            )},
            responses = {@OpenApiResponse(status = STATUS_200,
                    content = {
                            @OpenApiContent(type = Formats.JSONV2, from = TimeSeriesIdentifierDescriptors.class)
                    }),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the time series identifier descriptors were not found."),
                    @OpenApiResponse(status = STATUS_501, description = "request format is not "
                            + "implemented")}, description = "Returns CWMS timeseries identifier descriptor"
            + "Data", tags = {TAG})
    @Override
    public void getAll(Context ctx) {
        String cursor = ctx.queryParamAsClass(PAGE, String.class).getOrDefault("");
        int pageSize =
                ctx.queryParamAsClass(PAGE_SIZE, Integer.class).getOrDefault(DEFAULT_PAGE_SIZE);

        try (final Timer.Context ignored = markAndTime(GET_ALL)){
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesIdentifierDescriptorDao dao = new TimeSeriesIdentifierDescriptorDao(dsl);
            String office = ctx.queryParam(OFFICE);
            String idRegex = ctx.queryParam(TIMESERIES_ID_REGEX);

            TimeSeriesIdentifierDescriptors descriptors =
                    dao.getTimeSeriesIdentifiers(cursor, pageSize, office, idRegex);

            String formatHeader = ctx.header(Header.ACCEPT);
            if (Formats.DEFAULT.equals(formatHeader)) {
                // parseHeaderAndQueryParm normally defaults to JSONV1 when the input is */*
                formatHeader = Formats.JSONV2;
            }
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

            String result = Formats.format(contentType, descriptors);

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
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(from = TimeSeriesIdentifierDescriptor.class, type =
                                            Formats.JSONV2)
                            }
                    ),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the timeseries identifier descriptor was not found."),
                    @OpenApiResponse(status = STATUS_501, description = "request format is not "
                            + "implemented")},
            description = "Retrieves requested timeseries identifier descriptor", tags = {TAG})
    @Override
    public void getOne(Context ctx, @NotNull String timeseriesId) {

        try (final Timer.Context ignored = markAndTime(GET_ONE)){
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesIdentifierDescriptorDao dao = new TimeSeriesIdentifierDescriptorDao(dsl);
            String office = ctx.queryParam(OFFICE);

            String formatHeader = ctx.header(Header.ACCEPT);
            if (Formats.DEFAULT.equals(formatHeader)) {
                // parseHeaderAndQueryParm normally defaults to JSONV1 when the input is */*
                formatHeader = Formats.JSONV2;
            }

            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

            Optional<TimeSeriesIdentifierDescriptor> grp = dao.getTimeSeriesIdentifier(office, timeseriesId);
            if (grp.isPresent()) {
                String result = Formats.format(contentType, grp.get());

                ctx.result(result).contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            } else {
                CdaError re = new CdaError("Unable to find identifier based on parameters "
                        + "given");
                logger.info(() -> re + System.lineSeparator() + "for request " + ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }
        }
    }

    @OpenApi(
            description = "Create new TimeSeriesIdentifierDescriptor",
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = TimeSeriesIdentifierDescriptor.class, type = Formats.JSONV2),
                            @OpenApiContent(from = TimeSeriesIdentifierDescriptor.class, type = Formats.XMLV2)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                            description = "Create will fail if provided ID already exists. Default: true")
            },
            method = HttpMethod.POST,
            tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(CREATE)){
            DSLContext dsl = getDslContext(ctx);

            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSONV2;
            String body = ctx.body();

            TimeSeriesIdentifierDescriptor tsid = deserialize(body, formatHeader);

            TimeSeriesIdentifierDescriptorDao dao = new TimeSeriesIdentifierDescriptorDao(dsl);

            // these could be made optional queryParams
            boolean versioned = false;
            Number numForwards = null;
            Number numBackwards = null;
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            dao.create(tsid, versioned, numForwards, numBackwards, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED);
        } catch (JsonProcessingException ex) {
            CdaError re = new CdaError("Failed to process create request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    public static TimeSeriesIdentifierDescriptor deserialize(String body, String format) throws JsonProcessingException {
        TimeSeriesIdentifierDescriptor retval;
        if (ContentType.equivalent(Formats.JSONV2, format)) {
            ObjectMapper om = JsonV2.buildObjectMapper();
            retval = om.readValue(body, TimeSeriesIdentifierDescriptor.class);
        } else if (ContentType.equivalent(Formats.XMLV2,format)) {
            JacksonXmlModule module = new JacksonXmlModule();
            module.setDefaultUseWrapper(false);
            ObjectMapper om = new XmlMapper(module);
            retval = om.readValue(body, TimeSeriesIdentifierDescriptor.class);
        } else {
            throw new IllegalArgumentException("Unsupported format: " + format);
        }

        return retval;
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
                    @OpenApiParam(name = INTERVAL_OFFSET, type = Long.class, description = "The offset into the data interval in minutes.  "
                            + "If specified and a new timeseries-id is also specified both will be passed to a "
                            + "rename operation.  May also be passed to update operation."),
                    @OpenApiParam(name = SNAP_FORWARD, type = Long.class, description = "The new snap forward tolerance in minutes. This specifies how many minutes before the expected data time that data will be considered to be on time."),
                    @OpenApiParam(name = SNAP_BACKWARD, type = Long.class, description = "The new snap backward tolerance in minutes. This specifies how many minutes after the expected data time that data will be considered to be on time."),
                    @OpenApiParam(name = ACTIVE, type = Boolean.class, description = "'True' or 'true' if the time series is active")
            }, tags = {TAG}
    )
    @Override
    public void update(Context ctx, @NotNull String timeseriesId) {

        String office = requiredParam(ctx, OFFICE);
        String newTimeseriesId = ctx.queryParam(TIMESERIES_ID);
        Long intervalOffset = ctx.queryParamAsClass(INTERVAL_OFFSET, Long.class).getOrDefault(null);

        List<String> updateKeys = Arrays.asList(SNAP_FORWARD, SNAP_BACKWARD, ACTIVE, INTERVAL_OFFSET);

        Map<String, List<String>> paramMap = ctx.queryParamMap();
        List<String> foundUpdateKeys = updateKeys.stream()
                .filter(paramMap::containsKey)
                .collect(Collectors.toList());

        if (!foundUpdateKeys.isEmpty() && newTimeseriesId != null) {
            throw new IllegalArgumentException("Cannot specify a new timeseries-id and any of the"
                    + " following update parameters: " + foundUpdateKeys);
        }
        

        try (final Timer.Context ignored = markAndTime(UPDATE)){
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesIdentifierDescriptorDao dao = new TimeSeriesIdentifierDescriptorDao(dsl);

            if (foundUpdateKeys.isEmpty()) {
                // basic rename.
                dao.rename(office, timeseriesId, newTimeseriesId, intervalOffset);
            } else {
                Long forward = ctx.queryParamAsClass(SNAP_FORWARD, Long.class).getOrDefault(null);
                Long backward = ctx.queryParamAsClass(SNAP_BACKWARD, Long.class).getOrDefault(null);
                boolean active = ctx.queryParamAsClass(ACTIVE, Boolean.class).getOrDefault(true);

                dao.update(office, timeseriesId, intervalOffset, forward, backward, active);
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
                    @OpenApiParam(name = METHOD,  required = true, description = "Specifies the delete method used.",
                            type = JooqDao.DeleteMethod.class)
            },
            description = "Deletes requested timeseries identifier",
            method = HttpMethod.DELETE, tags = {TAG}
           )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String timeseriesId) {

        JooqDao.DeleteMethod method = ctx.queryParamAsClass(METHOD, JooqDao.DeleteMethod.class).get();

        String office = requiredParam(ctx, OFFICE);

        try (final Timer.Context ignored = markAndTime(DELETE)){
            DSLContext dsl = getDslContext(ctx);

            logger.log(Level.FINE, "Deleting timeseries:{0} from office:{1}", new Object[]{timeseriesId, office});
            TimeSeriesIdentifierDescriptorDao dao = new TimeSeriesIdentifierDescriptorDao(dsl);
            dao.delete(office, timeseriesId, method);

            ctx.status(HttpServletResponse.SC_OK);

        } catch (DataAccessException ex) {
            CdaError re = new CdaError("Internal Error");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

    }

}
