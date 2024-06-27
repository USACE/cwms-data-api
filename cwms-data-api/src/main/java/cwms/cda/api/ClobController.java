package cwms.cda.api;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.ClobDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.Clob;
import cwms.cda.data.dto.Clobs;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.http.HttpResponseException;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;
import java.util.Objects;
import java.util.Optional;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;


public class ClobController implements CrudHandler {
    private static final FluentLogger log = FluentLogger.forEnclosingClass();
    private static final int DEFAULT_PAGE_SIZE = 20;
    public static final String TAG = "Clob";
    public static final String TEXT_PLAIN = "text/plain";
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;



    public ClobController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = ClobController.class.getName();
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
                                + "the response."),
                @OpenApiParam(name = PAGE_SIZE,
                        type = Integer.class,
                        description = "How many entries per page returned. Default "
                                + DEFAULT_PAGE_SIZE + "."),
                @OpenApiParam(name = INCLUDE_VALUES,
                        type = Boolean.class,
                        description = "Do you want the value associated with this particular "
                                + "clob (default: false)"),
                @OpenApiParam(name = LIKE,
                        description = "Posix <a href=\"regexp.html\">regular expression</a> "
                                + "matching against the id")
            },
            responses = {@OpenApiResponse(status = STATUS_200,
                    description = "A list of clobs.",
                    content = {
                        @OpenApiContent(type = Formats.JSONV2, from = Clobs.class),
                        @OpenApiContent(type = Formats.XMLV2, from = Clobs.class)
                    })
            },
            tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            String office = ctx.queryParam(OFFICE);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, Clobs.class);

            String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                    String.class, "", metrics, name(ClobController.class.getName(), GET_ALL));

            if (!CwmsDTOPaginated.CURSOR_CHECK.invoke(cursor)) {
                ctx.json(new CdaError("cursor or page passed in but failed validation"))
                        .status(HttpCode.BAD_REQUEST);
                return;
            }

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE}, Integer.class, DEFAULT_PAGE_SIZE, metrics,
                    name(ClobController.class.getName(), GET_ALL));

            boolean includeValues = queryParamAsClass(ctx, new String[]{INCLUDE_VALUES},
                    Boolean.class, false, metrics,
                    name(ClobController.class.getName(), GET_ALL));
            String like = ctx.queryParamAsClass(LIKE, String.class).getOrDefault(".*");

            ClobDao dao = new ClobDao(dsl);
            Clobs clobs = dao.getClobs(cursor, pageSize, office, includeValues, like);
            String result = Formats.format(contentType, clobs);

            ctx.result(result);
            ctx.contentType(contentType.toString());
            requestResultSize.update(result.length());
        }
    }


    @OpenApi(
            description = "Get a single clob.  "
                + "If the accept header is set to " + TEXT_PLAIN + ", the raw value is returned as the response body. "
                + "Responses to " + TEXT_PLAIN + " requests are streamed and support the Range header.  "
                + "When the accept header is set to " + Formats.JSONV2 + " the clob will be returned as a serialized Clob "
                + "object with fields for office-id, id, description and value.",
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the owning office."),
                @OpenApiParam(name = CLOB_ID, description = "If this _query_ parameter is provided the id _path_ parameter "
                    + "is ignored and the value of the query parameter is used.   "
                    + "Note: this query parameter is necessary for id's that contain '/' or other special "
                    + "characters.  Because of abuse even properly escaped '/' in url paths are blocked.  "
                    + "When using this query parameter a valid path parameter must still be provided for the request"
                    + " to be properly routed.  If your clob id contains '/' you can't specify the clob-id query "
                    + "parameter and also specify the id path parameter because firewall and/or server rules will "
                    + "deny the request even though you are specifying this override. \"ignored\" is suggested.")
            },
            responses = {@OpenApiResponse(status = STATUS_200,
                    description = "Returns requested clob.",
                    content = {
                        @OpenApiContent(type = Formats.JSONV2, from = Clob.class),
                        @OpenApiContent(type = TEXT_PLAIN),
                    })
            },
            tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String clobId) {

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {

            String idQueryParam = ctx.queryParam(CLOB_ID);
            if (idQueryParam != null) {
                clobId = idQueryParam;
            }
            String formatHeader = ctx.header(Header.ACCEPT);

            DSLContext dsl = getDslContext(ctx);
            ClobDao dao = new ClobDao(dsl);
            String office = ctx.queryParam(OFFICE);

            if (TEXT_PLAIN.equals(formatHeader)) {
                // useful cmd:  curl -X 'GET' 'http://localhost:7000/cwms-data/clobs/encoded?office=SPK&id=%2FTIME%20SERIES%20TEXT%2F6261044'
                // -H 'accept: text/plain' --header "Range: bytes=20000-40000"
                dao.getClob(clobId, office, c -> {
                    if (c == null) {
                        ctx.status(HttpServletResponse.SC_NOT_FOUND).json(new CdaError("Unable to find "
                                + "clob based on given parameters"));
                    } else {
                        ctx.seekableStream(c.getAsciiStream(), TEXT_PLAIN, c.length());
                    }
                });
            } else {
                Optional<Clob> optAc = dao.getByUniqueName(clobId, office);

                if (optAc.isPresent()) {
                    ContentType contentType = Formats.parseHeader(formatHeader, Clob.class);

                    Clob clob = optAc.get();
                    String result = Formats.format(contentType, clob);

                    ctx.contentType(contentType.toString());
                    ctx.result(result);

                    requestResultSize.update(result.length());
                } else {
                    ctx.status(HttpServletResponse.SC_NOT_FOUND).json(new CdaError("Unable to find "
                            + "clob based on given parameters"));
                }
            }
        }
    }

    @OpenApi(
            description = "Create new Clob",
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = Clob.class, type = Formats.JSONV2),
                        @OpenApiContent(from = Clob.class, type = Formats.XMLV2)
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
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);

            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSONV2;

            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);

            ContentType contentType = Formats.parseHeader(formatHeader, Clob.class);
            Clob clob = Formats.parseContent(contentType, ctx.bodyAsInputStream(), Clob.class);
            ClobDao dao = new ClobDao(dsl);
            dao.create(clob, failIfExists);
            ctx.status(HttpCode.CREATED);
        }
    }

    @OpenApi(
            pathParams = {
                @OpenApiParam(name = CLOB_ID, required = true,
                            description = "Specifies the id of the clob to be updated"),
            },
            queryParams = {
                @OpenApiParam(name = IGNORE_NULLS, type = Boolean.class,
                        description = "If true, null and empty fields in the provided clob "
                                + "will be ignored and the existing value of those fields "
                                + "left in place. Default: true")
            },
            requestBody = @OpenApiRequestBody(
                content = {
                    @OpenApiContent(from = Clob.class, type = Formats.JSONV2),
                    @OpenApiContent(from = Clob.class, type = Formats.XMLV2)
                },
                    required = true),
            description = "Update clob",
            method = HttpMethod.PATCH,
            tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String clobId) {

        boolean ignoreNulls = ctx.queryParamAsClass(IGNORE_NULLS, Boolean.class).getOrDefault(true);

        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);

            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSON;
            ClobDao dao = new ClobDao(dsl);
            ContentType contentType = Formats.parseHeader(formatHeader, Clob.class);
            Clob clob = Formats.parseContent(contentType, ctx.bodyAsInputStream(), Clob.class);

            if (clob.getOfficeId() == null) {
                throw new HttpResponseException(HttpCode.BAD_REQUEST.getStatus(),
                        "An office is required in the request body when updating a clob");
            }

            clob = fillOutClob(clob, clobId);

            if (!Objects.equals(clob.getId(), clobId)) {
                throw new HttpResponseException(HttpCode.BAD_REQUEST.getStatus(),
                        "Clob id in body does not match id in path");
            }
            dao.update(clob, ignoreNulls);
        }
    }

    /**
     * Fills out the clob with the id and office if they are not already set.
     * @param clob The clob to fill out
     * @param reqId The id to set if the clob id is null
     * @return The clob with the id and office set
     */
    private Clob fillOutClob(Clob clob, String reqId) {
        Clob retval = clob;

        if (clob != null && (clob.getId() == null || clob.getOfficeId() == null)) {
            String id = clob.getId();
            if (id == null) {
                id = reqId;
            }

            retval = new Clob(id, clob.getOfficeId(), clob.getDescription(), clob.getValue());
        }

        return retval;
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = CLOB_ID, required = true,
                            description = "Specifies the id of the clob to be deleted"),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true,
                            description = "Specifies the office of the clob.")
            },
            description = "Delete clob",
            method = HttpMethod.DELETE,
            tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String clobId) {
        String office = requiredParam(ctx, OFFICE);

        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            ClobDao dao = new ClobDao(dsl);
            dao.delete(office, clobId);
        }
    }

}
