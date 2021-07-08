package cwms.radar.api;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.data.dao.ClobDao;
import cwms.radar.data.dao.JooqDao;
import cwms.radar.data.dto.Clob;
import cwms.radar.data.dto.Clobs;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
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
                    description = "Do you want the value assosciated with this particular clob (default: false)"
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
                      ),
                      @OpenApiResponse(status="501",description = "The format requested is not implemented"),
                      @OpenApiResponse(status="400", description = "Invalid Parameter combination")
                    },
        tags = {"Clob"}
    )
    @Override
    public void getAll(Context ctx) {
        getAllRequests.mark();
        try(
                final Timer.Context timeContext = getOneRequestTime.time();
                DSLContext dsl = getDslContext(ctx)
        ) {
            String office = ctx.queryParam("office");
            Optional<String> officeOpt = Optional.ofNullable(office);

            String cursor = ctx.queryParam("cursor",String.class,ctx.queryParam("page",String.class,"").getValue()).getValue();
            int pageSize = ctx.queryParam("pageSize",Integer.class,ctx.queryParam("pagesize",String.class,Integer.toString(defaultPageSize)).getValue()).getValue();

            boolean includeValues = ctx.queryParam("includeValues",Boolean.class,"false").getValue().booleanValue();
            String like = ctx.queryParam("like",".*");

            String formatParm = ctx.queryParam("format","");
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm);

            ClobDao dao = new ClobDao(dsl);
            Clobs clobs = dao.getClobs(cursor, pageSize, officeOpt, includeValues, like);
            String result = Formats.format(contentType,clobs);

            ctx.result(result);
            ctx.contentType(contentType.toString());
            requestResultSize.update(result.length());

        } catch ( FormattingException fe ){
            logger.log(Level.SEVERE,"failed to format data",fe);
            if( fe.getCause() instanceof IOException ){
                ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                ctx.result("server error");
            } else {
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.result("Invalid Format Options");
            }
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
                      ),
                      @OpenApiResponse(status="501",description = "The format requested is not implemented"),
                      @OpenApiResponse(status="400", description = "Invalid Parameter combination")
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
            Clob ac = optAc.orElse(null);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");

            String result = Formats.format(contentType, ac);

            ctx.contentType(contentType.toString());
            ctx.result(result);

            requestResultSize.update(result.length());
        }  catch( FormattingException fe ){
            logger.log(Level.SEVERE,"failed to format data",fe);
            if( fe.getCause() instanceof IOException ){
                ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                ctx.result("server error");
            } else {
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.result("Invalid Format Options");
            }
        }
    }




    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String clobId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String clobId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
