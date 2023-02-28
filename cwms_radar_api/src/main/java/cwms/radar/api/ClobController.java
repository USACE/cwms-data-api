package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.flogger.FluentLogger;

import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.ClobDao;
import cwms.radar.data.dao.JooqDao;
import cwms.radar.data.dto.Clob;
import cwms.radar.data.dto.Clobs;
import cwms.radar.data.dto.CwmsDTOPaginated;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import cwms.radar.formatters.json.JsonV2;
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
import java.io.StringReader;
import java.util.Objects;
import java.util.Optional;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class ClobController implements CrudHandler {
    private static final FluentLogger log = FluentLogger.forEnclosingClass();
    private static final int defaultPageSize = 20;
    public static final String CLOB_ID = "clob-id";
    public static final String TAG = "Clob";
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    public static final String OFFICE = "office";

    public ClobController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = ClobController.class.getName();
        requestResultSize = this.metrics.histogram((name(className, "results", "size")));
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
                    @OpenApiParam(name = "page",
                            description = "This end point can return a lot of data, this "
                                    + "identifies where in the request you are. This is an opaque"
                                    + " value, and can be obtained from the 'next-page' value in "
                                    + "the response."
                    ),
                    @OpenApiParam(name = "cursor",
                            deprecated = true,
                            description = "Deprecated. Use 'page' instead."
                    ),
                    @OpenApiParam(name = "page-size",
                            type = Integer.class,
                            description = "How many entries per page returned. Default "
                                    + defaultPageSize + "."
                    ),
                    @OpenApiParam(name = "pageSize",
                            deprecated = true,
                            type = Integer.class,
                            description = "Deprecated, use 'page-size' instead."
                    ),
                    @OpenApiParam(name = "include-values",
                            type = Boolean.class,
                            description = "Do you want the value associated with this particular "
                                    + "clob (default: false)"
                    ),
                    @OpenApiParam(name = "includeValues",
                            deprecated = true,
                            type = Boolean.class,
                            description = "Deprecated, use 'include-values' instead."
                    ),
                    @OpenApiParam(name = "like",
                            description = "Posix regular expression matching against the id"
                    )
            },
            responses = {@OpenApiResponse(status = "200",
                    description = "A list of clobs.",
                    content = {
                            @OpenApiContent(type = Formats.JSONV2, from = Clobs.class),
                            @OpenApiContent(type = Formats.XMLV2, from = Clobs.class)
                    }
            )
            },
            tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {

        try (
                final Timer.Context ignored = markAndTime("getAll");
                DSLContext dsl = getDslContext(ctx)
        ) {
            String office = ctx.queryParam(OFFICE);
            Optional<String> officeOpt = Optional.ofNullable(office);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");

            String cursor = Controllers.queryParamAsClass(ctx, new String[]{"page", "cursor"},
                    String.class, "", metrics, name(ClobController.class.getName(), "getAll"));

            if (!CwmsDTOPaginated.CURSOR_CHECK.invoke(cursor)) {
                ctx.json(new RadarError("cursor or page passed in but failed validation"))
                        .status(HttpCode.BAD_REQUEST);
                return;
            }

            int pageSize = Controllers.queryParamAsClass(ctx, new String[]{"page-size", "pageSize",
                            "pagesize"}, Integer.class, defaultPageSize, metrics,
                    name(ClobController.class.getName(), "getAll"));

            boolean includeValues = Controllers.queryParamAsClass(ctx, new String[]{"include-values",
                            "includeValues"}, Boolean.class, false, metrics,
                    name(ClobController.class.getName(), "getAll"));
            String like = ctx.queryParamAsClass("like", String.class).getOrDefault(".*");

            ClobDao dao = new ClobDao(dsl);
            Clobs clobs = dao.getClobs(cursor, pageSize, officeOpt, includeValues, like);
            String result = Formats.format(contentType, clobs);

            ctx.result(result);
            ctx.contentType(contentType.toString());
            requestResultSize.update(result.length());

        }
    }


    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, description = "Specifies the owning office."),
            },
            responses = {@OpenApiResponse(status = "200",
                    description = "Returns requested clob.",
                    content = {
                            @OpenApiContent(type = Formats.JSONV2, from = Clob.class),
                    }
            )
            },
            tags = {TAG}
    )
    @Override
    public void getOne(Context ctx, @NotNull String clobId) {

        try (
                final Timer.Context ignored = markAndTime("getOne");
                DSLContext dsl = getDslContext(ctx)
        ) {
            ClobDao dao = new ClobDao(dsl);
            Optional<String> office = Optional.ofNullable(ctx.queryParam(OFFICE));
            Optional<Clob> optAc = dao.getByUniqueName(clobId, office);

            if (optAc.isPresent()) {
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");

                Clob clob = optAc.get();
                String result = Formats.format(contentType, clob);

                ctx.contentType(contentType.toString());
                ctx.result(result);

                requestResultSize.update(result.length());
            } else {
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(new RadarError("Unable to find "
                        + "clob based on given parameters"));
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
                    @OpenApiParam(name = "fail-if-exists", type = Boolean.class,
                            description = "Create will fail if provided ID already exists. Default: true")
            },
            method = HttpMethod.POST,
            tags = {TAG}
    )
    @Override
    public void create(Context ctx) {
        try (final Timer.Context ignored = markAndTime("create");
             DSLContext dsl = getDslContext(ctx)) {
            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSON;

            boolean failIfExists = ctx.queryParamAsClass("fail-if-exists", Boolean.class).getOrDefault(true);

            try {
                Clob clob = deserialize(ctx.body(), formatHeader);

                if (clob.getOffice() == null) {
                    throw new FormattingException("An officeId is required when creating a clob");
                }

                if (clob.getId() == null) {
                    throw new FormattingException("An Id is required when creating a clob");
                }

                if (clob.getValue() == null || clob.getValue().isEmpty()) {
                    throw new FormattingException("A non-empty value field is required when "
                            + "creating a clob");
                }

                ClobDao dao = new ClobDao(dsl);
                dao.create(clob, failIfExists);
                ctx.status(HttpCode.CREATED);
            } catch (JsonProcessingException e) {
                throw new HttpResponseException(HttpCode.NOT_ACCEPTABLE.getStatus(),"Unable to parse request body");
            }
        }
    }

    public Clob deserialize(String body, String formatHeader) throws JsonProcessingException {
        ObjectMapper om = getObjectMapperForFormat(formatHeader);
        return om.readValue(body, Clob.class);
    }


    private static ObjectMapper getObjectMapperForFormat(String format) {
        ObjectMapper om;
        log.atInfo().log("Getting objectmapper for format '%s'",format);
        if (ContentType.equivalent(Formats.XMLV2,format)) {
            JacksonXmlModule module = new JacksonXmlModule();
            module.setDefaultUseWrapper(false);
            om = new XmlMapper(module);
        } else if (ContentType.equivalent(Formats.JSONV2,format)) {
            om = JsonV2.buildObjectMapper();
        } else {
            FormattingException fe = new FormattingException("Format specified is not currently supported for Clob");
            log.atFine().withCause(fe).log("Format %s not supported",format);
            throw fe;
        }
        om.registerModule(new JavaTimeModule());
        return om;
    }

    public static Clob deserializeJAXB(String body) throws JAXBException {
        JAXBContext jaxbContext = JAXBContext.newInstance(Clob.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        return (Clob) unmarshaller.unmarshal(new StringReader(body));
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = CLOB_ID, required = true,
                            description = "Specifies the id of the clob to be updated"),
            },
            queryParams = {
                    @OpenApiParam(name = "ignore-nulls",
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

        boolean ignoreNulls = ctx.queryParamAsClass("ignore-nulls", Boolean.class).getOrDefault(true);

        try (final Timer.Context ignored = markAndTime("update");
             DSLContext dsl = getDslContext(ctx)) {

            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSON;
            ClobDao dao = new ClobDao(dsl);

            try {
                Clob clob = deserialize(ctx.body(), formatHeader);

                if (clob.getOffice() == null) {
                    throw new HttpResponseException(HttpCode.BAD_REQUEST.getStatus(),
                    "An office is required in the request body when updating a clob");
                }

                clob = fillOutClob(clob, clobId);

                if (!Objects.equals(clob.getId(), clobId)) {
                    throw new HttpResponseException(HttpCode.BAD_REQUEST.getStatus(),
                            "Clob id in body does not match id in path");
                }

                dao.update(clob, ignoreNulls);
            } catch (JsonProcessingException e) {
                throw new HttpResponseException(HttpCode.NOT_ACCEPTABLE.getStatus(),
                        "Unable to parse request body");
            }

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

        if (clob != null && (clob.getId() == null || clob.getOffice() == null)) {
            String id = clob.getId();
            if (id == null) {
                id = reqId;
            }

            retval = new Clob(id, clob.getOffice(), clob.getDescription(), clob.getValue());
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
    public void delete(Context ctx, @NotNull String clobId) {
        String office = ctx.queryParam(OFFICE);

        try (final Timer.Context ignored = markAndTime("delete");
             DSLContext dsl = getDslContext(ctx)) {
            ClobDao dao = new ClobDao(dsl);
            dao.delete(office, clobId);
        }
    }

}
