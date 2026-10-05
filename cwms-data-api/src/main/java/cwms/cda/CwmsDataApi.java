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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.flogger.FluentLogger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import cwms.cda.api.auth.userlists.UserListController;
import cwms.cda.api.errors.ApplicationException;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.ExceptionTraceSupport;
import cwms.cda.data.dao.rss.QueueManager;
import cwms.cda.openapi.OpenApiSchemeProcessor;
import cwms.cda.security.Authenticator;
import cwms.cda.security.CdaAccessManager;
import cwms.cda.security.Role;
import cwms.cda.validation.ValidationSetup;
import io.javalin.Javalin;
import io.javalin.compression.CompressionStrategy;
import io.javalin.config.JavalinConfig;
import io.javalin.config.RoutesConfig;
import io.javalin.security.RouteRole;
import io.javalin.http.BadRequestResponse;
import io.javalin.openapi.plugin.OpenApiPlugin;
import io.javalin.plugin.bundled.RateLimitPlugin;
import io.javalin.plugin.bundled.RouteOverviewPlugin;
import io.opentelemetry.api.trace.Span;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;

import java.io.File;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.sql.DataSource;

import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.http.entity.ContentType;
import org.eclipse.jetty.ee10.webapp.WebAppContext;
import org.eclipse.jetty.http.HttpCookie;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.session.DefaultSessionCache;
import org.eclipse.jetty.session.NullSessionDataStore;
import org.eclipse.jetty.ee10.servlet.SessionHandler;
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
public final class CwmsDataApi {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    // based on https://bitbucket.hecdev.net/projects/CWMS/repos/cwms_aaa/browse/IntegrationTests/src/test/resources/sql/load_testusers.sql
    public static final String CWMS_USERS_ROLE = "CWMS Users";
    public static final String CAC_USER = "cac_auth";
    public static final String DATABASE = "database";
    public static final String IS_NEW_LRTS = "X-CWMS-LRTS-Formatting";

    // The VERSION should match the gradle version but not contain the patch version.
    // For example 2.4 not 2.4.13
    private static String VERSION;

    public static final String APPLICATION_TITLE = "CWMS Data API";
    public static final String PROVIDER_KEY = "cwms.dataapi.access.provider";
    public static final String DEFAULT_OFFICE_KEY = "cwms.dataapi.default.office";
    public static final String DEFAULT_PROVIDER = "MultipleAccessManager";

    public static String getApiVersion() {
        return VERSION != null ? VERSION : "Not Yet Known";
    }

    private final String appContext;
    private final MetricRegistry metrics = new MetricRegistry(); 
    private final Javalin app;

    public static void main(String[] args)
    {

        final var appContext = args.length > 0 ? args[0] : "cwms-data";
        final var uiPath = args.length > 1 ? args[1] : null;

        var ds = buildDataSource();
        var api = CwmsDataApi.builder()
                             .withContext(appContext)
                             .withPort(7000)
                             .withUiWar(new File(uiPath))
                             .withDataSource(ds)
                             .build();
        api.start();
    }
    

    @SuppressWarnings({"java:S125","java:S2095"}) // closed in destroy handler
    private CwmsDataApi(int port, String context, File uiWar, DataSource ds, SessionHandler sessionHandler) {
        this.appContext = context;
        logger.atInfo().log("Initializing Javalin.");
        CwmsDataApi.VERSION = obtainFullVersion();
        logger.atInfo().log("Initializing CWMS Data API Version:  " + VERSION);

        var totalRequests = metrics.meter("cwms.dataapi.total_requests");
        ObjectMapper om = new ObjectMapper();
        om.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        om.registerModule(new JavaTimeModule());

        PolicyFactory sanitizer = new HtmlPolicyBuilder().disallowElements("<script>").toFactory();

        final Authenticator authenticator = new Authenticator();
        final OpenApiSchemeProcessor schemeProcessor = new OpenApiSchemeProcessor(authenticator);

        final var cdaAccessManager = new CdaAccessManager();
        app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
            config.http.generateEtags = true;
            config.http.compressionStrategy = CompressionStrategy.NONE;
            getOpenApiOptions(config, appContext);            
            config.requestLogger.http((ctx, ms) -> logger.atFinest().log(ctx.toString()));
            config.router.contextPath = appContext;
            ValidationSetup.registerValidation(config.validation);
            config.registerPlugin(new RateLimitPlugin());
            if (uiWar != null) {
                config.jetty.modifyServer(server -> {
                    var war = new WebAppContext();
                    war.setContextPath("");
                    war.setWar(uiWar.getAbsolutePath());
                    var handlers = new ContextHandlerCollection();
                    handlers.setHandlers(server.getHandler(), war);
                    server.setHandler(handlers);
                });
            }
            config.jetty.modifyServletContextHandler(handler -> {
                handler.setSessionHandler(sessionHandler);
            });
            
            config.routes.beforeMatched(cdaAccessManager);
            config.appData(CwmsDataApiAttributes.POLICY_FACTORY_KEY, sanitizer);
            config.appData(CwmsDataApiAttributes.OBJECT_MAPPER_KEY, om);
            config.appData(CwmsDataApiAttributes.SCHEME_PROCESSOR_KEY, schemeProcessor);
            config.appData(CwmsDataApiAttributes.DATA_SOURCE_KEY, ds);
            config.appData(CwmsDataApiAttributes.OFFICE_ID_KEY, officeFromContext(context));
            config.registerPlugin(new RouteOverviewPlugin(o -> {}));
            config.routes
                .before(authenticator)
                .before(ctx -> totalRequests.mark())
                .before(ctx -> {
                    ctx.attribute("sanitizer", sanitizer);
                    ctx.header("X-Content-Type-Options", "nosniff");
                    ctx.header("X-Frame-Options", "SAMEORIGIN");
                    ctx.header("X-XSS-Protection", "1; mode=block");
                    // A given endpoint can override this, but otherwise we
                    // don't want javalin or jetty to even try to guess
                    ctx.res().setCharacterEncoding(null);
                })
                .before(ctx -> {
                    // now that we can get the generic route, update the name.
                    var span = Span.current();
                    span.updateName(ctx.method() + " " + ctx.endpoint().path);
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
                                                        errResponse.getIncidentIdentifier(), ctx.req().getRequestURI());
                    ctx.status(500);
                    ctx.contentType(ContentType.APPLICATION_JSON.toString());
                    ctx.json(errResponse);
                })
                .exception(Exception.class, (e, ctx) -> {
                    CdaError errResponse = ExceptionTraceSupport.buildError(ctx, "System Error", e);
                    logger.atWarning().withCause(e).log("error on request[%s]: %s",
                            errResponse.getIncidentIdentifier(), ctx.req().getRequestURI());
                    ctx.status(500);
                    ctx.contentType(ContentType.APPLICATION_JSON.toString());
                    ctx.json(errResponse);
                })
                .options("/*", ctx -> {
                    // Respond with a 200 OK status for preflight checks.
                    // It is expected that the firewall in front of the API
                    // will handle any CORS headers.
                    ctx.status(200);
                });
                configureRoutes(config.routes, metrics, cdaAccessManager);
            });
        QueueManager.ensureRssSubscribers(ds);
        logger.atInfo().log("Javalin initialized.");
    }

    public void start() {
        app.start();
    }

    public void stop() {
        app.stop();   
    }

    public int getPort() {
        return app.port();
    }

    private void configureRoutes(RoutesConfig routes, MetricRegistry metrics, CdaAccessManager cdaAccessManager) {
        RouteRole[] requiredRoles = {new Role(CWMS_USERS_ROLE)};
        ApiServletRouteConfiguration.configureRoutes(routes, metrics, requiredRoles, cdaAccessManager);
    }

    private  String obtainFullVersion() {
        return "99.99.99"; // TODO: actually get
    }

    private void getOpenApiOptions(JavalinConfig config, String appContext) {
        Info applicationInfo = new Info().title(APPLICATION_TITLE).version(CwmsDataApi.getApiVersion())
                .description("CWMS REST API for Data Retrieval");

        String provider = CdaAccessManager.class.getSimpleName();


        config.registerPlugin(new OpenApiPlugin(openapi -> {
            openapi.prettyOutputEnabled = true;
            openapi.documentationPath = "/swagger-docs";
            openapi.withDefinitionConfiguration((v,builder) -> {
                builder.info(info -> info.title(APPLICATION_TITLE).version(CwmsDataApi.VERSION));
                builder.server(server -> server.url(appContext));
            });
        }));

        
        
        //TODO: The rest
              
            //                        .addSecurityItem(new SecurityRequirement().addList(provider))
        
        // ops.path("/swagger-docs")
        //     .responseModifier((ctx,api) -> {
        //         schemeProcessor.apply(ctx, api);
        //         api.getPaths().forEach((key,path) -> {
        //             setSecurityRequirements(key,path, schemeProcessor.getSecurityRequirements());
        //             setUserListTags(key, path);
        //             // yeah, we really need to figure out how to update everything,
        //             // this is supported as an annotation in newer versions.
        //             if (key.startsWith("/rss")) {
        //                 path.getGet().getResponses().forEach((p, r) -> {
        //                     var retryAfter = new io.swagger.v3.oas.models.headers.Header();
        //                     retryAfter.description(
        //                         "Amount of time (in seconds) to wait before making the next request.");
        //                     r.addHeaderObject(Header.RETRY_AFTER, retryAfter);
        //                 });
        //             }
        //         });
        //         Map<String, Class<? extends CwmsCsvDTO>> schemaToClass = new HashMap<>();
        //         try (ScanResult scanResult = new ClassGraph()
        //                 .acceptPackages("cwms.cda.data.dto")
        //                 .scan()) {
        //             List<Class<CwmsCsvDTO>> csvDtoClasses = 
        //                 scanResult.getClassesImplementing(CwmsCsvDTO.class.getName())
        //                         .loadClasses(CwmsCsvDTO.class);
        //             for (Class<? extends CwmsCsvDTO> clazz : csvDtoClasses) {
        //                 schemaToClass.put(clazz.getSimpleName(), clazz);
        //             }
        //         }
        //         api.getPaths().values().forEach(pathItem -> {
        //             for (Operation op : pathItem.readOperations()) {
        //                 if (op.getResponses() != null) {
        //                     for (ApiResponse resp : op.getResponses().values()) {
        //                         if (resp.getContent() != null && resp.getContent().containsKey(Formats.CSV)) {
        //                             MediaType csvMedia = resp.getContent().get(Formats.CSV);
        //                             if (csvMedia.getSchema() != null && csvMedia.getSchema().get$ref() != null) {
        //                                 String ref = csvMedia.getSchema().get$ref();
        //                                 String schemaName = ref.substring(ref.lastIndexOf('/') + 1);
        //                                 @SuppressWarnings("unchecked")
        //                                 Class<? extends CwmsCsvDTO<?>> dtoClass =
        //                                     (Class<? extends CwmsCsvDTO<?>>) schemaToClass.get(schemaName);

        //                                 if (dtoClass != null) {
        //                                     csvMedia.setExample(CsvExampleGenerator.getExample(dtoClass));
        //                                 }
        //                             }
        //                         }
        //                     }
        //                 }
        //             }
        //         });
        //         return api;
        //     })
        //     .defaultDocumentation(doc -> {
        //         doc.json("500", CdaError.class);
        //         doc.json("400", CdaError.class);
        //         doc.json("401", CdaError.class);
        //         doc.json("403", CdaError.class);
        //         doc.json("404", CdaError.class);
        //         doc.json("429", CdaError.class);
        //         doc.header(IS_NEW_LRTS,
        //             Boolean.class,
        //             p -> p.description(
        //                 "If True, will use use the new 'Local Regular Time Series" 
        //                 + " naming scheme. For example 1DayLocal. Instead of the original"
        //                 + " PsuedoRegular based scheme, for example ~1DayLocal."
        //                 + " NOTE: this parameter only applies to the input and output of"
        //                 + " Time Series names. It is added to all endpoints and will be ignored" 
        //                 + " when not required. Default values is false if not set.")
        //         );
        //     })
        //     .activateAnnotationScanningFor("cwms.cda.api");
        // addEndpointExamples(ops);
        

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

    // @Override
    // protected void service(HttpServletRequest req, HttpServletResponse resp)
    //         throws IOException {
    //     totalRequests.mark();
    //     try {
    //         String office = officeFromContext(req.getContextPath());
    //         req.setAttribute(OFFICE_ID, office);
    //         //logger.atInfo().log("Connection user name is: %s")
    //         req.setAttribute(DATA_SOURCE, cwms);
    //         req.setAttribute(RAW_DATA_SOURCE,cwms);
    //         javalin.service(req, resp);
    //     } catch (Exception ex) {
    //         CdaError re = new CdaError("Major Database Issue");
    //         logger.atSevere().withCause(ex).log(re + " for url " + req.getRequestURI());
    //         resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    //         resp.setContentType(ContentType.APPLICATION_JSON.toString());
    //         try (PrintWriter out = resp.getWriter()) {
    //             ObjectMapper om = new ObjectMapper();
    //             out.println(om.writeValueAsString(re));
    //         }
    //     }
    // }

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

    /**
     * Initialize data source from properties or environment
     * 
     * TODO: environment.
     * @return
     */
    public static DataSource buildDataSource()
    {
        var dsConfig = new HikariConfig();
        dsConfig.setJdbcUrl(System.getProperty("CDA_JDBC_URL"));
        dsConfig.setUsername(System.getProperty("CDA_JDBC_USERNAME"));
        dsConfig.setPassword(System.getProperty("CDA_JDBC_PASSWORD"));
        dsConfig.setMaximumPoolSize(Integer.parseInt(System.getProperty("CDA_POOL_MAX_ACTIVE", "1")));
        return new HikariDataSource(dsConfig);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder {
        private int port = 7000;
        private String context = "/cwms-data";
        private File uiWar = null;
        private DataSource dataSource;

        private SessionHandler sessionManager = null;


        public Builder withPort(int port) {
            this.port = 7000;
            return this;
        }

        public Builder withContext(String context) {
            this.context = context;
            return this;
        }

        public Builder withUiWar(String uiWar) {
            this.uiWar = new File(uiWar);
            return this;
        }

        public Builder withUiWar(File uiWar) {
            this.uiWar = uiWar;
            return this;
        }

        public Builder withDataSource(DataSource dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public Builder withSessionManager(SessionHandler sessionManager) {
            this.sessionManager = sessionManager;
            return this;
        }

        public CwmsDataApi build()
        {
            if (sessionManager == null) {
                this.sessionManager = new SessionHandler();
                sessionManager.setSameSite(HttpCookie.SameSite.STRICT);
                sessionManager.setHttpOnly(true);
                sessionManager.setMaxInactiveInterval(900);
                var cache = new DefaultSessionCache(sessionManager);
                cache.setSessionDataStore(new NullSessionDataStore());
                sessionManager.setSessionCache(cache);
            }
            return new CwmsDataApi(port, context, uiWar, dataSource, sessionManager);
        }
    }
}
