package cwms.radar.api;

import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.data.dao.LocationCategoryDao;
import cwms.radar.data.dto.LocationCategory;
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

import cwms.radar.api.errors.RadarError;

public class LocationCategoryController implements CrudHandler
{
	public static final Logger logger = Logger.getLogger(LocationCategoryController.class.getName());

	private final MetricRegistry metrics;
	private final Meter getAllRequests;
	private final Timer getAllRequestsTime;
	private final Meter getOneRequest;
	private final Timer getOneRequestTime;
	private final Histogram requestResultSize;

	public LocationCategoryController(MetricRegistry metrics)
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
			@OpenApiParam(name = "office", description = "Specifies the owning office of the location category(ies) whose data is to be included in the response. If this field is not specified, matching location category information from all offices shall be returned."),
			},
			responses = {@OpenApiResponse(status = "200",
					content = {
						@OpenApiContent(isArray = true, from = LocationCategory.class, type = Formats.JSON)
					})
			},

			description = "Returns CWMS Location Category Data",
			tags = {"Location Categories"}
	)
	@Override
	public void getAll(Context ctx)
	{
		getAllRequests.mark();
		try(final Timer.Context timeContext = getAllRequestsTime.time();
			DSLContext dsl = getDslContext(ctx))
		{
			LocationCategoryDao dao = new LocationCategoryDao(dsl);
			String office = ctx.queryParam("office");

			List<LocationCategory> cats = dao.getLocationCategories(office);

			if( !cats.isEmpty()) {
				String formatHeader = ctx.header(Header.ACCEPT);
				ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "json");

				String result = Formats.format(contentType,cats);

				ctx.result(result).contentType(contentType.toString());
				requestResultSize.update(result.length());

				ctx.status(HttpServletResponse.SC_OK);
			} else {
				final RadarError re = new RadarError("Cannot requested location category for office provided");

                logger.info(() -> {
                    StringBuilder builder = new StringBuilder(re.toString())
                        .append("with url:" ).append(ctx.fullUrl());
                    return builder.toString();
                });
                ctx.json(re).status(HttpServletResponse.SC_NOT_FOUND);
			}

		}

	}

	@OpenApi(
			pathParams = {
					@OpenApiParam(name = "category-id", required = true, description = "Specifies the Category whose data is to be included in the response."),
			},
			queryParams = {
				@OpenApiParam(name = "office", required = true, description = "Specifies the owning office of the Location Category whose data is to be included in the response."),
			},
			responses = {
					@OpenApiResponse(status = "200",
							content = {
								@OpenApiContent(from = LocationCategory.class, type = Formats.JSON)
							}
					)
			},
			description = "Retrieves requested Location Category",
			tags = {"Location Categories"})
	@Override
	public void getOne(Context ctx, String categoryId)
	{
		getOneRequest.mark();
		try(final Timer.Context timeContext = getOneRequestTime.time();
			DSLContext dsl = getDslContext(ctx))
		{
			LocationCategoryDao dao = new LocationCategoryDao(dsl);
			String office = ctx.queryParam("office");

			Optional<LocationCategory> grp = dao.getLocationCategory(office, categoryId);
			if( grp.isPresent() ){
				String formatHeader = ctx.header(Header.ACCEPT);
				ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "json");

				String result = Formats.format(contentType,grp.get());

				ctx.result(result).contentType(contentType.toString());
				requestResultSize.update(result.length());

				ctx.status(HttpServletResponse.SC_OK);
			} else {
				final RadarError re = new RadarError("Cannot requested location category id");

                logger.info(() -> {
                    StringBuilder builder = new StringBuilder(re.toString())
                        .append("with url:" ).append(ctx.fullUrl());
                    return builder.toString();
                });
                ctx.json(re).status(HttpServletResponse.SC_NOT_FOUND);
			}

		}

	}

	@OpenApi(ignore = true)
	@Override
	public void create(Context ctx)
	{
		ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
	}

	@OpenApi(ignore = true)
	@Override
	public void update(Context ctx, String locationCode)
	{
		ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
	}

	@OpenApi(ignore = true)
	@Override
	public void delete(Context ctx, String locationCode)
	{
		ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
	}
}
