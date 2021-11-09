package cwms.radar.api;

import java.util.Optional;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.ClobDao;
import cwms.radar.data.dao.JooqDao;
import cwms.radar.data.dto.Clob;
import cwms.radar.data.dto.Clobs;
import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;

import org.jooq.DSLContext;

import static com.codahale.metrics.MetricRegistry.name;


/**
 *
 */
public class ClobController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(ClobController.class.getName());
    private static final int defaultPageSize = 20;
    private final MetricRegistry metrics;
    private final Meter getAllRequests;
    private final Timer getAllRequestsTime;
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Histogram requestResultSize;

    public ClobController(MetricRegistry metrics){
        this.metrics=metrics;
        String className = ClobController.class.getName();
        getAllRequests = this.metrics.meter(name(className,"getAll","count"));
        getAllRequestsTime = this.metrics.timer(name(className,"getAll","time"));
        getOneRequest = this.metrics.meter(name(className,"getOne","count"));
        getOneRequestTime = this.metrics.timer(name(className,"getOne","time"));
        requestResultSize = this.metrics.histogram((name(className,"results","size")));
    }

    protected DSLContext getDslContext(Context ctx)
    {
        return JooqDao.getDslContext(ctx);
    }

    @OpenApi(
            queryParams = {
                @OpenApiParam(name="office",
                            required=false,
                            description="Specifies the owning office. If this field is not specified, matching information from all offices shall be returned."),
                @OpenApiParam(name="page",
                            required = false,
                            description = "This end point can return a lot of data, this identifies where in the request you are. This is an opaque value, and can be obtained from the 'next-page' value in the response."
                ),
                @OpenApiParam(name="pageSize",
                            required=false,
                            type=Integer.class,
                            description = "How many entries per page returned. Default " + defaultPageSize + "."
                ),
                @OpenApiParam(name="includeValues",
                    required = false,
                    type = Boolean.class,
                    description = "Do you want the value associated with this particular clob (default: false)"
                ),
                @OpenApiParam(name="like",
                    required = false,
                    type = String.class,
                    description = "Posix regular expression describing the clob id's you want"
                )
            },
        responses = { @OpenApiResponse(status="200",
                                       description = "A list of clobs.",
                                       content = {
                                           @OpenApiContent( type = Formats.JSONV2, from = Clobs.class ),
                                           @OpenApiContent( type = Formats.XMLV2, from = Clobs.class )
                                       }
                      )
                    },
        tags = {"Clob"}
    )
    @Override
    public void getAll(Context ctx) {
        getAllRequests.mark();
        try(
                final Timer.Context timeContext = getAllRequestsTime.time();
                DSLContext dsl = getDslContext(ctx)
        ) {
            String office = ctx.queryParam("office");
            Optional<String> officeOpt = Optional.ofNullable(office);
            String formatParm = ctx.queryParamAsClass("format",String.class).getOrDefault("");
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm);

            String cursor = ctx.queryParamAsClass("cursor",String.class).allowNullable().get();
            cursor = cursor != null ? cursor : ctx.queryParamAsClass("page",String.class).getOrDefault("");
            if( CwmsDTOPaginated.CURSOR_CHECK.invoke(cursor) != true ) {
                ctx.json(new RadarError("cursor or page passed in but failed validation"))
                    .status(HttpCode.BAD_REQUEST);
                return;
            }
            int pageSize = ctx.queryParamAsClass("pageSize",Integer.class)
								.getOrDefault(
									ctx.queryParamAsClass("pagesize",Integer.class).getOrDefault(defaultPageSize)
								);

            boolean includeValues = ctx.queryParamAsClass("includeValues",Boolean.class).getOrDefault(false);
            String like = ctx.queryParamAsClass("like",String.class).getOrDefault(".*");



            ClobDao dao = new ClobDao(dsl);
            Clobs clobs = dao.getClobs(cursor, pageSize, officeOpt, includeValues, like);
            String result = Formats.format(contentType,clobs);

            ctx.result(result);
            ctx.contentType(contentType.toString());
            requestResultSize.update(result.length());

        }
    }



    @OpenApi(
            queryParams = {
            @OpenApiParam(name = "office", description = "Specifies the owning office."),
    },
        responses = { @OpenApiResponse(status="200",
                                       description = "Returns requested clob.",
                                       content = {
                                           @OpenApiContent(type = Formats.JSON, from = Clob.class ),
                                       }
                      )
                    },
        tags = {"Clob"}
    )
    @Override
    public void getOne(Context ctx, String clobId) {
        getOneRequest.mark();
        try(
                final Timer.Context timeContext = getOneRequestTime.time();
                DSLContext dsl = getDslContext(ctx)
        ) {
            ClobDao dao = new ClobDao(dsl);
            Optional<String> office = Optional.ofNullable(ctx.queryParam("office"));
            Optional<Clob> optAc = dao.getByUniqueName(clobId,  office);

            if( optAc.isPresent() ){
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");

                String result = Formats.format(contentType, optAc.get());

                ctx.contentType(contentType.toString());
                ctx.result(result);

                requestResultSize.update(result.length());
            } else {
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(new RadarError("Unable to find clob based on given parameters"));
            }



        }
    }




    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String clobId) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String clobId) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

}
