/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

package cwms.cda;

import static cwms.cda.openapi.ExampleUtils.addEndpointExamples;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.Controllers;
import cwms.cda.api.auth.userlists.UserListController;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.ApplicationException;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.ExceptionTraceSupport;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.rss.QueueManager;
import cwms.cda.data.dto.csv.CwmsCsvDTO;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.csv.CsvExampleGenerator;
import cwms.cda.openapi.OpenApiSchemeProcessor;
import cwms.cda.security.Authenticator;
import cwms.cda.security.CdaAccessManager;
import cwms.cda.security.Role;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import io.javalin.Javalin;
import io.javalin.core.JavalinConfig;
import io.javalin.core.security.RouteRole;
import io.javalin.core.util.Header;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.http.BadRequestResponse;
import io.javalin.http.JavalinServlet;
import io.javalin.plugin.openapi.OpenApiOptions;
import io.javalin.plugin.openapi.OpenApiPlugin;
import io.opentelemetry.api.trace.Span;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.servers.Server;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.Manifest;
import javax.annotation.Resource;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import org.apache.http.entity.ContentType;
import org.jooq.exception.DataAccessException;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;


/**
 * Setup all the information required so we can serve the request.
 *
 */
@WebServlet(urlPatterns = { "/catalog/*",
    "/auth/*",
    "/swagger-docs",
    "/timeseries/*",
    "/offices/*",
    "/states/*",
    "/counties/*",
    "/location/*",
    "/locations/*",
    "/entity/*",
    "/parameters/*",
    "/timezones/*",
    "/units/*",
    "/ratings/*",
    "/levels/*",
    "/level-refs/*",
    "/basins/*",
    "/streams/*",
    "/stream-locations/*",
    "/stream-reaches/*",
    "/measurements/*",
    "/published/*",
    "/blobs/*",
    "/clobs/*",
    "/pools/*",
    "/specified-levels/*",
    "/forecast-spec/*",
    "/forecast-instance/*",
    "/standard-text-id/*",
    "/projects/*",
    "/project-locks/*",
    "/project-lock-rights/*",
    "/properties/*",
    "/lookup-types/*",
    "/embankments/*",
    "/user/*",
    "/users/*",
    "/roles/*",
    "/version/*",
    "/rss/*",
    "/v2/*"
})
public class ApiServlet extends HttpServlet {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    // based on https://bitbucket.hecdev.net/projects/CWMS/repos/cwms_aaa/browse/IntegrationTests/src/test/resources/sql/load_testusers.sql
    public static final String CWMS_USERS_ROLE = "CWMS Users";
    public static final String CAC_USER = "cac_auth";
    /** Default OFFICE where needed. Based on context. e.g. /cwms-data -> HQ, /spk-data -> SPK */
    public static final String OFFICE_ID = "office_id";
    public static final String DATA_SOURCE = "data_source";
    public static final String RAW_DATA_SOURCE = "data_source";
    public static final String DATABASE = "database";
    public static final String IS_NEW_LRTS = "X-CWMS-LRTS-Formatting";

    // The VERSION should match the gradle version but not contain the patch version.
    // For example 2.4 not 2.4.13
    private static String VERSION;

    public static final String APPLICATION_TITLE = "CWMS Data API";
    public static final String PROVIDER_KEY_OLD = "radar.access.provider";
    public static final String PROVIDER_KEY = "cwms.dataapi.access.provider";
    public static final String DEFAULT_OFFICE_KEY = "cwms.dataapi.default.office";
    public static final String DEFAULT_PROVIDER = "MultipleAccessManager";

    private MetricRegistry metrics;
    private Meter totalRequests;

    private static final long serialVersionUID = 1L;

    JavalinServlet javalin = null;
    private final Authenticator authenticator = new Authenticator();
    private final OpenApiSchemeProcessor schemeProcessor = new OpenApiSchemeProcessor(authenticator);
    private String appContext;

    @Resource(name = "jdbc/CWMS3")
    DataSource cwms;
    private CdaAccessManager cdaAccessManager;

    public static String getApiVersion() {
        return VERSION != null ? VERSION : "Not Yet Known";
    }


    @Override
    public void destroy() {
        javalin.destroy();
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        if (VERSION == null) {
            ApiServlet.VERSION = obtainFullVersion(config);
        }
        logger.atInfo().log("Initializing CWMS Data API Version:  " + VERSION);
        metrics = (MetricRegistry)config.getServletContext()
                .getAttribute(MetricsServlet.METRICS_REGISTRY);
        totalRequests = metrics.meter("cwms.dataapi.total_requests");

        super.init(config);
    }

    @SuppressWarnings({"java:S125","java:S2095"}) // closed in destroy handler
    @Override
    public void init() {
        logger.atInfo().log("Initializing Javalin.");
        JavalinValidation.register(UnitSystem.class, UnitSystem::systemFor);
        JavalinValidation.register(JooqDao.DeleteMethod.class, Controllers::getDeleteMethod);

        ObjectMapper om = new ObjectMapper();
        om.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        om.registerModule(new JavaTimeModule());

        PolicyFactory sanitizer = new HtmlPolicyBuilder().disallowElements("<script>").toFactory();
        appContext = this.getServletContext().getContextPath();
        cdaAccessManager = new CdaAccessManager();
        javalin = Javalin.createStandalone(config -> {
            config.defaultContentType = "application/json";
            getOpenApiOptions(config);
            config.autogenerateEtags = true;
            config.requestLogger((ctx, ms) -> logger.atFinest().log(ctx.toString()));
            config.accessManager(cdaAccessManager);
        })
                .attribute("PolicyFactory", sanitizer)
                .attribute("ObjectMapper", om)
                .attribute("schemeProcessor", schemeProcessor)
                .before(authenticator)
                .before(ctx -> {
                    ctx.attribute("sanitizer", sanitizer);
                    ctx.header("X-Content-Type-Options", "nosniff");
                    ctx.header("X-Frame-Options", "SAMEORIGIN");
                    ctx.header("X-XSS-Protection", "1; mode=block");
                })
                .before(ctx -> {
                    // now that we can get the generic route, update the name.
                    var span = Span.current();
                    span.updateName(ctx.method() + " " + ctx.matchedPath());
                })
                .exception(ApplicationException.class, (e, ctx) -> {
                    CdaError re = ExceptionTraceSupport.buildError(ctx, e.getCdaErrorMessage(),
                            e.getSource(), e.getDetails(), e);
                    if (e.getLoggerLevel().isPresent()) {
                        logger.at(e.getLoggerLevel().get()).withCause(e).log(re.toString());
                    }
                    ctx.status(e.getCdaHttpErrorCode()).json(re);
                })
                .exception(UnsupportedOperationException.class, (e, ctx) -> {
                    final CdaError re = ExceptionTraceSupport.buildError(ctx, "Not Implemented", e);
                    logger.atWarning().withCause(e)
                            .log("%s for request: %s", re, ctx.fullUrl());
                    ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(re);
                })
                .exception(BadRequestResponse.class, (e, ctx) -> {
                    CdaError re = ExceptionTraceSupport.buildError(ctx, "Bad Request",
                        "User Input", new HashMap<>(e.getDetails()), e);
                    logger.atInfo().withCause(e).log(re.toString());
                    ctx.status(e.getStatus()).json(re);
                })
                .exception(IllegalArgumentException.class, (e, ctx) -> {
                    CdaError re = ExceptionTraceSupport.buildError(ctx, "Bad Request", e);
                    logger.atInfo().withCause(e).log(re.toString());
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(DateTimeException.class, (e, ctx) -> {
                    CdaError re = ExceptionTraceSupport.buildError(ctx, e.getMessage(), e);
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(DataAccessException.class, (e, ctx) -> {
                    // Whatever Dao is causing this exception to be thrown should be modified.
                    // The preferred pattern is for the Dao to catch DataAccessExceptions exceptions
                    // and for the dao to inspect the Oracle error code or error message as necessary
                    // to transform DataAccessExceptions (and their SQLException causes)
                    // into specific and appropriate exceptions with
                    // messages that are helpful and meaningful to end-users.

                    // CdaError does not include the Oracle exception message b/c this block catches
                    // all unhandled DataAccessExceptions and we don't know what is in the message
                    // it is unknown if the message would be safe/appropriate for users to see.
                    CdaError errResponse = ExceptionTraceSupport.buildError(ctx, "Database Error", e);
                    logger.atWarning().withCause(e).log("error on request[%s]: %s",
                                                        errResponse.getIncidentIdentifier(), ctx.req.getRequestURI());
                    ctx.status(500);
                    ctx.contentType(ContentType.APPLICATION_JSON.toString());
                    ctx.json(errResponse);
                })
                .exception(Exception.class, (e, ctx) -> {
                    CdaError errResponse = ExceptionTraceSupport.buildError(ctx, "System Error", e);
                    logger.atWarning().withCause(e).log("error on request[%s]: %s",
                            errResponse.getIncidentIdentifier(), ctx.req.getRequestURI());
                    ctx.status(500);
                    ctx.contentType(ContentType.APPLICATION_JSON.toString());
                    ctx.json(errResponse);
                })
                .routes(this::configureRoutes)
                .options("/*", ctx -> {
                    // Respond with a 200 OK status for preflight checks.
                    // It is expected that the firewall in front of the API
                    // will handle any CORS headers.
                    ctx.status(200);
                })
                .javalinServlet();
        QueueManager.ensureRssSubscribers(cwms);
        logger.atInfo().log("Javalin initialized.");
    }

    private void configureRoutes() {
        RouteRole[] requiredRoles = {new Role(CWMS_USERS_ROLE)};
        ApiServletRouteConfiguration.configureRoutes(metrics, requiredRoles, cdaAccessManager);
    }

    private String obtainFullVersion(ServletConfig servletConfig) throws ServletException {
        String relativeWarPath = "/META-INF/MANIFEST.MF";
        String absoluteDiskPath = servletConfig.getServletContext().getRealPath(relativeWarPath);
        Path path = Paths.get(absoluteDiskPath);

        try (InputStream inputStream = Files.newInputStream(path)) {
            Manifest manifest = new Manifest(inputStream);
            return manifest.getMainAttributes().getValue("build-version");
        } catch (IOException e) {
            throw new ServletException("Error obtaining servlet version", e);
        }
    }

    private void getOpenApiOptions(JavalinConfig config) {
        Info applicationInfo = new Info().title(APPLICATION_TITLE).version(ApiServlet.getApiVersion())
                .description("CWMS REST API for Data Retrieval");

        String provider = CdaAccessManager.class.getSimpleName();

        List<Server> servers = new ArrayList<>();
        servers.add(new Server().url(appContext));
        OpenApiOptions ops =
            new OpenApiOptions(
                () -> new OpenAPI()
                                   .servers(servers)
                                   .info(applicationInfo)
                                   .addSecurityItem(new SecurityRequirement().addList(provider))
        );
        ops.path("/swagger-docs")
            .responseModifier((ctx,api) -> {
                schemeProcessor.apply(ctx, api);
                api.getPaths().forEach((key,path) -> {
                    setSecurityRequirements(key,path, schemeProcessor.getSecurityRequirements());
                    setUserListTags(key, path);
                    // yeah, we really need to figure out how to update everything, 
                    // this is supported as an annotation in newer versions.
                    if (key.startsWith("/rss")) {
                        path.getGet().getResponses().forEach((p, r) -> {
                            var retryAfter = new io.swagger.v3.oas.models.headers.Header();
                            retryAfter.description(
                                "Amount of time (in seconds) to wait before making the next request.");
                            r.addHeaderObject(Header.RETRY_AFTER, retryAfter);
                        });
                    }
                });
                Map<String, Class<? extends CwmsCsvDTO>> schemaToClass = new HashMap<>();
                try (ScanResult scanResult = new ClassGraph()
                        .acceptPackages("cwms.cda.data.dto")
                        .scan()) {
                    List<Class<CwmsCsvDTO>> csvDtoClasses = 
                        scanResult.getClassesImplementing(CwmsCsvDTO.class.getName())
                                .loadClasses(CwmsCsvDTO.class);
                    for (Class<? extends CwmsCsvDTO> clazz : csvDtoClasses) {
                        schemaToClass.put(clazz.getSimpleName(), clazz);
                    }
                }
                api.getPaths().values().forEach(pathItem -> {
                    for (Operation op : pathItem.readOperations()) {
                        if (op.getResponses() != null) {
                            for (ApiResponse resp : op.getResponses().values()) {
                                if (resp.getContent() != null && resp.getContent().containsKey(Formats.CSV)) {
                                    MediaType csvMedia = resp.getContent().get(Formats.CSV);
                                    if (csvMedia.getSchema() != null && csvMedia.getSchema().get$ref() != null) {
                                        String ref = csvMedia.getSchema().get$ref();
                                        String schemaName = ref.substring(ref.lastIndexOf('/') + 1);
                                        @SuppressWarnings("unchecked")
                                        Class<? extends CwmsCsvDTO<?>> dtoClass =
                                            (Class<? extends CwmsCsvDTO<?>>) schemaToClass.get(schemaName);

                                        if (dtoClass != null) {
                                            csvMedia.setExample(CsvExampleGenerator.getExample(dtoClass));
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
                return api;
            })
            .defaultDocumentation(doc -> {
                doc.json("500", CdaError.class);
                doc.json("400", CdaError.class);
                doc.json("401", CdaError.class);
                doc.json("403", CdaError.class);
                doc.json("404", CdaError.class);
                doc.json("429", CdaError.class);
                doc.header(IS_NEW_LRTS,
                    Boolean.class,
                    p -> p.description(
                        "If True, will use use the new 'Local Regular Time Series" 
                        + " naming scheme. For example 1DayLocal. Instead of the original"
                        + " PsuedoRegular based scheme, for example ~1DayLocal."
                        + " NOTE: this parameter only applies to the input and output of"
                        + " Time Series names. It is added to all endpoints and will be ignored" 
                        + " when not required. Default values is false if not set.")
                );
            })
            .activateAnnotationScanningFor("cwms.cda.api");
        addEndpointExamples(ops);
        config.registerPlugin(new OpenApiPlugin(ops));

    }

    private static void setSecurityRequirements(String key, PathItem path,List<SecurityRequirement> secReqs) {
        /* clear the lock icon from the GET handlers to reduce user confusion */
        logger.atFinest().log("setting security constraints for " + key);
        if ((path.getGet() != null && path.getGet().getSecurity() != null)) {
            setSecurity(path.getGet(), secReqs);
        } else {
            setSecurity(path.getGet(), new ArrayList<>());
        }
        setSecurity(path.getDelete(),secReqs);
        setSecurity(path.getPost(), secReqs);
        setSecurity(path.getPut(), secReqs);
        setSecurity(path.getPatch(),secReqs);
    }

    static void setUserListTags(String key, PathItem path) {
        if (key.startsWith("/user/list")) {
            path.readOperations().forEach(operation ->
                    operation.setTags(List.of(UserListController.TAG)));
        }
    }

    private static void setSecurity(Operation op,List<SecurityRequirement> reqs) {
        if (op != null) {
            op.setSecurity(reqs);
        }
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp)
            throws IOException {
        totalRequests.mark();
        try {
            String office = officeFromContext(req.getContextPath());
            req.setAttribute(OFFICE_ID, office);
            //logger.atInfo().log("Connection user name is: %s")
            req.setAttribute(DATA_SOURCE, cwms);
            req.setAttribute(RAW_DATA_SOURCE,cwms);
            javalin.service(req, resp);
        } catch (Exception ex) {
            CdaError re = new CdaError("Major Database Issue");
            logger.atSevere().withCause(ex).log(re + " for url " + req.getRequestURI());
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.setContentType(ContentType.APPLICATION_JSON.toString());
            try (PrintWriter out = resp.getWriter()) {
                ObjectMapper om = new ObjectMapper();
                out.println(om.writeValueAsString(re));
            }
        }
    }

    /**
     * Retrieve the specific office name.
     * @param contextPath applicatio context path
     * @return default office id for this instance.
     */
    public static String officeFromContext(String contextPath) {
        String office = contextPath.split("-")[0].replaceFirst("/","");
        if (office.isEmpty() || office.equalsIgnoreCase("cwms")) {
            office = "HQ";
        }
        return System.getProperty(DEFAULT_OFFICE_KEY, office).toUpperCase();
    }
}
