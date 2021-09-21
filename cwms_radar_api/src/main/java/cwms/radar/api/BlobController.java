package cwms.radar.api;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.BlobDao;
import cwms.radar.data.dao.JooqDao;
import cwms.radar.data.dto.Blob;
import cwms.radar.data.dto.Blobs;
import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.core.validation.ValidationError;
import io.javalin.core.validation.ValidationException;
import io.javalin.core.validation.Validator;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.eclipse.jetty.http.HttpStatus;
import org.jooq.DSLContext;

import static com.codahale.metrics.MetricRegistry.name;


/**
 *
 */
public class BlobController implements CrudHandler {

    private static final int defaultPageSize = 20;
    private final MetricRegistry metrics;
    private final Meter getAllRequests;
    private final Timer getAllRequestsTime;
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Histogram requestResultSize;

    public BlobController(MetricRegistry metrics){
        this.metrics=metrics;
        String className = BlobController.class.getName();
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
                @OpenApiParam(name="like",
                    required = false,
                    type = String.class,
                    description = "Posix regular expression describing the blob id's you want"
                )
            },
        responses = { @OpenApiResponse(status="200",
                                       description = "A list of blobs.",
                                       content = {
                                           @OpenApiContent( type = Formats.JSONV2, from = Blobs.class ),
                                           @OpenApiContent( type = Formats.XMLV2, from = Blobs.class )
                                       }
                      )
                    },
        tags = {"Blob"}
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

            String like = ctx.queryParamAsClass("like",String.class).getOrDefault(".*");

            String formatParm = ctx.queryParamAsClass("format",String.class).getOrDefault("");
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm);

            BlobDao dao = new BlobDao(dsl);
            List<Blob> blobList = dao.getAll(officeOpt, like);

            Blobs blobs = new Blobs.Builder(cursor, pageSize, 0).addAll(blobList).build();
            String result = Formats.format(contentType,blobs);

            ctx.result(result);
            ctx.contentType(contentType.toString());
            requestResultSize.update(result.length());
        }
    }

    @OpenApi(
            queryParams = {
            @OpenApiParam(name = "office", description = "Specifies the owning office."),
    },
        tags = {"Blob"}
    )
    @Override
    public void getOne(Context ctx, String blobId) {
        getOneRequest.mark();
        try(
                final Timer.Context timeContext = getOneRequestTime.time();
                DSLContext dsl = getDslContext(ctx)
        ) {
            BlobDao dao = new BlobDao(dsl);
            String officeQP = ctx.queryParam("office");
            Optional<String> office = Optional.ofNullable(officeQP);
            Optional<Blob> optAc = dao.getByUniqueName(blobId,  office);

            if( optAc.isPresent() ){
                Blob blob = optAc.get();

                ctx.contentType(blob.getMediaTypeId());
                byte[] value = blob.getValue();
                ctx.result(value);

                requestResultSize.update(value.length);
            } else {
                ctx.status(HttpStatus.NOT_FOUND_404).json(new RadarError("Unable to find blob based on given parameters"));
            }

        }
    }


    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String blobId) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String blobId) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

}
