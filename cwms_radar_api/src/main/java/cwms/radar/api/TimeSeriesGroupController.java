package cwms.radar.api;

import java.util.List;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.TimeSeriesGroupDao;
import cwms.radar.data.dto.TimeSeriesGroup;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jooq.DSLContext;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.JooqDao.getDslContext;

public class TimeSeriesGroupController implements CrudHandler
{
	public static final Logger logger = Logger.getLogger(TimeSeriesGroupController.class.getName());

	private final MetricRegistry metrics;
	private final Meter getAllRequests;
	private final Timer getAllRequestsTime;
	private final Meter getOneRequest;
	private final Timer getOneRequestTime;
	private final Histogram requestResultSize;

	public TimeSeriesGroupController(MetricRegistry metrics)
	{
		this.metrics = metrics;
		String className = this.getClass().getName();
		getAllRequests = this.metrics.meter(name(className, "getAll", "count"));
		getAllRequestsTime = this.metrics.timer(name(className, "getAll", "time"));
		getOneRequest = this.metrics.meter(name(className, "getOne", "count"));
		getOneRequestTime = this.metrics.timer(name(className, "getOne", "time"));
		requestResultSize = this.metrics.histogram((name(className, "results", "size")));
	}

	@OpenApi(queryParams = {
			@OpenApiParam(name = "office", description = "Specifies the owning office of the timeseries group(s) whose data is to be included in the response. If this field is not specified, matching timeseries groups information from all offices shall be returned."),
			},
			responses = {
			@OpenApiResponse(status = "200",
					content = {@OpenApiContent(isArray = true, from = TimeSeriesGroup.class, type = Formats.JSON)
							//							@OpenApiContent(isArray = true, from = TabV1TimeseriesGroup.class, type = Formats.TAB ),
							//							@OpenApiContent(isArray = true, from = CsvV1TimeseriesGroup.class, type = Formats.CSV )
					}

			),
					@OpenApiResponse(status = "404", description = "Based on the combination of inputs provided the timeseries group(s) were not found."),
					@OpenApiResponse(status = "501", description = "request format is not implemented")}, description = "Returns CWMS Timeseries Groups Data", tags = {"Timeseries Groups"})
	@Override
	public void getAll(Context ctx)
	{
		getAllRequests.mark();
		try(final Timer.Context timeContext = getAllRequestsTime.time();
			DSLContext dsl = getDslContext(ctx))
		{
			TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);
			String office = ctx.queryParam("office");

			List<TimeSeriesGroup> grps = dao.getTimeSeriesGroups(office);

			String formatHeader = ctx.header(Header.ACCEPT);
			ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "json");

			String result = Formats.format(contentType,grps);

			ctx.result(result).contentType(contentType.toString());
			requestResultSize.update(result.length());

			ctx.status(HttpServletResponse.SC_OK);
		}

	}

	@OpenApi(
			pathParams = {
					@OpenApiParam(name = "group-id", required = true, description = "Specifies the timeseries group whose data is to be included in the response")
			},
			queryParams = {
					@OpenApiParam(name = "office", required = true, description = "Specifies the owning office of the timeseries group whose data is to be included in the response."),
					@OpenApiParam(name = "category-id", required = true, description = "Specifies the category containing the timeseries group whose data is to be included in the response."),
			},
			responses = {
					@OpenApiResponse(status = "200", content = {
							@OpenApiContent(from = TimeSeriesGroup.class, type = Formats.JSON),
					}

			)},
			description = "Retrieves requested timeseries group", tags = {"Timeseries Groups"})
	@Override
	public void getOne(Context ctx, String groupId)
	{
		getOneRequest.mark();
		try(final Timer.Context timeContext = getOneRequestTime.time();
			DSLContext dsl = getDslContext(ctx))
		{
			TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);
			String office = ctx.queryParam("office");
			String categoryId = ctx.queryParam("category-id");

			String formatHeader = ctx.header(Header.ACCEPT);
			ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "json");

			TimeSeriesGroup group = null;
			List<TimeSeriesGroup> timeSeriesGroups = dao.getTimeSeriesGroups(office, categoryId, groupId);
			if(timeSeriesGroups != null && !timeSeriesGroups.isEmpty())
			{
				if(timeSeriesGroups.size() == 1)
				{
					group = timeSeriesGroups.get(0);
				}
				else
				{
					// An error. [office, categoryId, groupId] should have, at most, one match
					String message = String.format(
							"Multiple TimeSeriesGroups returned from getTimeSeriesGroups "
									+ "for:%s category:%s groupId:%s At most one match was expected. Found:%s",
							office, categoryId, groupId, timeSeriesGroups);
					throw new IllegalArgumentException(message);
				}
			}
			if( group != null){
				String result = Formats.format(contentType, group);


				ctx.result(result);
				ctx.contentType(contentType.toString());
				requestResultSize.update(result.length());

				ctx.status(HttpServletResponse.SC_OK);
			} else {
				RadarError re = new RadarError("Unable to find group based on parameters given");
				logger.info( () -> {
					return new StringBuilder()
					.append( re.toString()).append(System.lineSeparator())
					.append( "for request ").append( ctx.fullUrl() )
					.toString();
				});
				ctx.status(HttpServletResponse.SC_NOT_FOUND).json( re );
			}

		}

	}

	@OpenApi(ignore = true)
	@Override
	public void create(Context ctx)
	{
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@OpenApi(ignore = true)
	@Override
	public void update(Context ctx, String groupId)
	{
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@OpenApi(ignore = true)
	@Override
	public void delete(Context ctx, String groupId)
	{
		throw new UnsupportedOperationException("Not supported yet.");
	}
}
