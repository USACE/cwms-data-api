package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.CURSOR;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.LIKE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.queryParamAsClass;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.BlobDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.Blob;
import cwms.cda.data.dto.Blobs;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import javax.servlet.http.HttpServletResponse;
import kotlin.Triple;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


/**
 *
 */
public class BlobController implements CrudHandler {

    private static final int defaultPageSize = 20;

    private final MetricRegistry metrics;


    private final Histogram requestResultSize;

    public BlobController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = BlobController.class.getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE,
                            description = "Specifies the owning office. If this field is not "
                                    + "specified, matching information from all offices shall be "
                                    + "returned."),
                    @OpenApiParam(name = PAGE,
                            description = "This end point can return a lot of data, this "
                                    + "identifies where in the request you are. This is an opaque"
                                    + " value, and can be obtained from the 'next-page' value in "
                                    + "the response."
                    ),
                    @OpenApiParam(name = PAGE_SIZE,
                            type = Integer.class,
                            description = "How many entries per page returned. Default "
                                    + defaultPageSize + "."
                    ),
                    @OpenApiParam(name = LIKE,
                            description = "Posix <a href=\"regexp.html\">regular expression</a> describing the blob id's you want"
                    )
            },
            responses = {@OpenApiResponse(status = STATUS_200,
                    description = "A list of blobs.",
                    content = {
                            @OpenApiContent(type = Formats.JSONV2, from = Blobs.class),
                            @OpenApiContent(type = Formats.XMLV2, from = Blobs.class)
                    }
            )
            },
            tags = {"Blob"}
    )
    @Override
    public void getAll(@NotNull Context ctx) {

        try ( final Timer.Context timeContext = markAndTime(GET_ALL)) {
                DSLContext dsl = getDslContext(ctx);
            String office = ctx.queryParam(OFFICE);
            Optional<String> officeOpt = Optional.ofNullable(office);

            String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                    String.class, "", metrics, name(BlobController.class.getName(), GET_ALL));

            if (!CwmsDTOPaginated.CURSOR_CHECK.invoke(cursor)) {
                ctx.json(new CdaError("cursor or page passed in but failed validation"))
                        .status(HttpCode.BAD_REQUEST);
                return;
            }

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE},
                    Integer.class, defaultPageSize, metrics,
                    name(BlobController.class.getName(), GET_ALL));

            String like = ctx.queryParamAsClass(LIKE, String.class).getOrDefault(".*");

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");

            BlobDao dao = new BlobDao(dsl);
            List<Blob> blobList = dao.getAll(officeOpt, like);

            Blobs blobs = new Blobs.Builder(cursor, pageSize, 0).addAll(blobList).build();
            String result = Formats.format(contentType, blobs);

            ctx.result(result);
            ctx.contentType(contentType.toString());
            requestResultSize.update(result.length());
        }
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, description = "Specifies the owning office."),
            },
            tags = {"Blob"}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String blobId) {

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            BlobDao dao = new BlobDao(dsl);
            String officeQP = ctx.queryParam(OFFICE);
            Optional<String> office = Optional.ofNullable(officeQP);

            Consumer<Triple<InputStream, Long, String>> tripleConsumer = triple -> {
                InputStream is = triple.getFirst();
                Long size = triple.getSecond();
                String mediaType = triple.getThird();

                if (is == null) {
                    ctx.status(HttpServletResponse.SC_NOT_FOUND).json(new CdaError("Unable to find "
                            + "blob based on given parameters"));
                } else {
                    requestResultSize.update(size);
                    ctx.seekableStream(is, mediaType, size);
                }
            };
            if (office.isPresent()) {
                dao.getBlob(blobId, office.get(), tripleConsumer);
            } else {
                dao.getBlob(blobId, tripleConsumer);
            }
        }
    }


    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, @NotNull String blobId) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, @NotNull String blobId) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

}
