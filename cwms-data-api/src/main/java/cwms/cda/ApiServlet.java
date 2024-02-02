/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

import static io.javalin.apibuilder.ApiBuilder.crud;
import static io.javalin.apibuilder.ApiBuilder.get;
import static io.javalin.apibuilder.ApiBuilder.prefixPath;
import static io.javalin.apibuilder.ApiBuilder.sse;
import static io.javalin.apibuilder.ApiBuilder.staticInstance;
import static io.javalin.apibuilder.ApiBuilder.ws;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.BasinController;
import cwms.cda.api.BlobController;
import cwms.cda.api.CatalogController;
import cwms.cda.api.ClobAsyncController;
import cwms.cda.api.ClobController;
import cwms.cda.api.Controllers;
import cwms.cda.api.CountyController;
import cwms.cda.api.LevelsController;
import cwms.cda.api.LocationCategoryController;
import cwms.cda.api.LocationController;
import cwms.cda.api.LocationGroupController;
import cwms.cda.api.OfficeController;
import cwms.cda.api.ParametersController;
import cwms.cda.api.PingController;
import cwms.cda.api.PoolController;
import cwms.cda.api.RatingController;
import cwms.cda.api.RatingMetadataController;
import cwms.cda.api.RatingSpecController;
import cwms.cda.api.RatingTemplateController;
import cwms.cda.api.SpecifiedLevelController;
import cwms.cda.api.StateController;
import cwms.cda.api.TimeSeriesCategoryController;
import cwms.cda.api.TimeSeriesController;
import cwms.cda.api.TimeSeriesGroupController;
import cwms.cda.api.TimeSeriesIdentifierDescriptorController;
import cwms.cda.api.TimeZoneController;
import cwms.cda.api.UnitsController;
import cwms.cda.api.auth.ApiKeyController;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.AlreadyExists;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.DeleteConflictException;
import cwms.cda.api.errors.FieldException;
import cwms.cda.api.errors.InvalidItemException;
import cwms.cda.api.errors.JsonFieldsException;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.formatters.FormattingException;
import cwms.cda.security.CwmsAuthException;
import cwms.cda.security.Role;
import cwms.cda.spi.AccessManagers;
import cwms.cda.spi.CdaAccessManager;
import io.javalin.Javalin;
import io.javalin.apibuilder.CrudFunction;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.apibuilder.CrudHandlerKt;
import io.javalin.core.JavalinConfig;
import io.javalin.core.security.RouteRole;
import io.javalin.core.util.Header;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.http.BadRequestResponse;
import io.javalin.http.Handler;
import io.javalin.http.JavalinServlet;
import io.javalin.http.staticfiles.Location;
import io.javalin.plugin.openapi.OpenApiOptions;
import io.javalin.plugin.openapi.OpenApiPlugin;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;
import javax.annotation.Resource;
import javax.management.ServiceNotFoundException;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import oracle.jdbc.pool.OracleDataSource;
import org.apache.http.entity.ContentType;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.StdErrLog;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jetbrains.annotations.NotNull;
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
        "/parameters/*",
        "/timezones/*",
        "/units/*",
        "/ratings/*",
        "/levels/*",
        "/basins/*",
        "/blobs/*",
        "/clobs/*",
        "/pools/*",
        "/specified-levels/*",
        "/sse/*",
        "/ws/*",
        "/ping/*"
},
        asyncSupported = true
)
public class ApiServlet extends HttpServlet {

    public static final FluentLogger logger = FluentLogger.forEnclosingClass();

    // based on https://bitbucket.hecdev.net/projects/CWMS/repos/cwms_aaa/browse/IntegrationTests/src/test/resources/sql/load_testusers.sql
    public static final String CWMS_USERS_ROLE = "CWMS Users";
    /** Default OFFICE where needed. Based on context. e.g. /cwms-data -> HQ, /spk-data -> SPK */
    public static final String OFFICE_ID = "office_id";
    public static final String DATA_SOURCE = "data_source";
    public static final String RAW_DATA_SOURCE = "data_source";
    public static final String DATABASE = "database";

    // The VERSION should match the gradle version but not contain the patch version.
    // For example 2.4 not 2.4.13
    public static final String VERSION = "3.0";
    public static final String APPLICATION_TITLE = "CWMS Data API";
    public static final String PROVIDER_KEY_OLD = "radar.access.provider";
    public static final String PROVIDER_KEY = "cwms.dataapi.access.provider";
    public static final String DEFAULT_OFFICE_KEY = "cwms.dataapi.default.office";
    public static final String DEFAULT_PROVIDER = "MultipleAccessManager";

    private MetricRegistry metrics = new MetricRegistry();
    private Meter totalRequests;

    private static final long serialVersionUID = 1L;

    JavalinServlet javalinServlet = null;
    public Javalin javalin = null;

    @Resource(name = "jdbc/CWMS3")
    DataSource cwms;


    public ApiServlet() {
        super();
    }

    @Override
    public void destroy() {
        if(javalinServlet != null) {
            javalinServlet.destroy();
        }
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        logger.atInfo().log("Initializing CWMS Data API Version:  " + obtainFullVersion(config));
        metrics = (MetricRegistry)config.getServletContext()
                .getAttribute(MetricsServlet.METRICS_REGISTRY);

        if(metrics == null){
            metrics = new MetricRegistry();
            config.getServletContext().setAttribute(MetricsServlet.METRICS_REGISTRY, metrics);
        }

        totalRequests = metrics.meter("cwms.dataapi.total_requests");
        super.init(config);
    }

    @SuppressWarnings({"java:S125","java:S2095"}) // closed in destroy handler
    @Override
    public void init() throws ServletException {
        super.init();
        JavalinValidation.register(UnitSystem.class, UnitSystem::systemFor);
        JavalinValidation.register(JooqDao.DeleteMethod.class, Controllers::getDeleteMethod);

        javalin = createJavalin();

        javalinServlet = javalin.javalinServlet();

    }

    public Javalin createJavalin() {

        ObjectMapper om = new ObjectMapper();
        om.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        om.registerModule(new JavaTimeModule());

        PolicyFactory sanitizer = new HtmlPolicyBuilder().disallowElements("<script>").toFactory();

        List<String> externalRootDir = getExternalRootDirs();

        return Javalin.create(config -> {
                    config.defaultContentType = "application/json";
                    config.contextPath = "/cwms-data";//this.getServletContext().getContextPath();
                    getOpenApiOptions(config);
                    config.autogenerateEtags = true;
                    config.requestLogger((ctx, ms) -> logger.atFinest().log(ctx.toString()));

                    for (String dir : externalRootDir) {
                        config.addStaticFiles(staticFiles -> {
                            staticFiles.hostedPath = "/";                   // change to host files on a subpath, like '/assets'
                            staticFiles.directory = dir;                    // the directory where your files are located
                            staticFiles.location = Location.EXTERNAL;      // Location.CLASSPATH (jar) or Location.EXTERNAL (file system)
                        });
                    }

                })
                .attribute("PolicyFactory", sanitizer)
                .attribute("ObjectMapper", om)
                .wsBefore(wsConfig -> {  ////// #1
                    logger.atInfo().log("#1--wsBefore");  // This actually happens when the server starts -before any ws requests.
                    wsConfig.onConnect(wsConnectHandler -> {
                        logger.atInfo().log("#2--onConnect");  ///// #2  This happens once a ws connection is made.
                        wsConnectHandler.attribute(OFFICE_ID, "SPK");
                        wsConnectHandler.attribute(DATA_SOURCE, cwms);
                        wsConnectHandler.attribute(RAW_DATA_SOURCE,cwms);
                    });
                })
                .wsBefore("/ws/clob", wsConfig -> {
                                        logger.atInfo().log("#3--wsBefore /ws/clob");  // This also happens at start, after #1
                                        wsConfig.onConnect(wsConnectHandler -> {
                                            logger.atInfo().log("#4--wsBefore /ws/clob onConnect"); //// #4 this happens when a ws connection is made, shows up after #2
                                        });
                })
                .before(ctx -> {
                    ctx.attribute("sanitizer", sanitizer);

                    //// We don't hit the protected void service(HttpServletRequest req, HttpServletResponse resp) method when run from main so setting attributes here for Jetty ws test.
                    ctx.attribute(OFFICE_ID, "SPK");
                    ctx.attribute(DATA_SOURCE, cwms);
                    ctx.attribute(RAW_DATA_SOURCE,cwms);
                    ////

                    ctx.header("X-Content-Type-Options", "nosniff");
                    ctx.header("X-Frame-Options", "SAMEORIGIN");
                    ctx.header("X-XSS-Protection", "1; mode=block");
                })
                .exception(FormattingException.class, (fe, ctx) -> {
                    final CdaError re = new CdaError("Formatting error:" + fe.getMessage());

                    if (fe.getCause() instanceof IOException) {
                        ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    } else {
                        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);
                    }
                    logger.atSevere().withCause(fe)
                            .log("%s for request: %s", re, ctx.fullUrl());
                    ctx.json(re);
                })
                .exception(UnsupportedOperationException.class, (e, ctx) -> {
                    final CdaError re = CdaError.notImplemented();
                    logger.atWarning().withCause(e)
                            .log("%s for request: %s", re, ctx.fullUrl());
                    ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(re);
                })
                .exception(BadRequestResponse.class, (e, ctx) -> {
                    CdaError re = new CdaError("Bad Request", e.getDetails());
                    logger.atInfo().withCause(e).log(re.toString());
                    ctx.status(e.getStatus()).json(re);
                })
                .exception(IllegalArgumentException.class, (e, ctx) -> {
                    CdaError re = new CdaError("Bad Request");
                    logger.atInfo().withCause(e).log(re.toString());
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(InvalidItemException.class, (e, ctx) -> {
                    CdaError re = new CdaError("Bad Request.");
                    logger.atInfo().withCause(e).log(re.toString());
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(AlreadyExists.class, (e, ctx) -> {
                    CdaError re = new CdaError("Already Exists.");
                    logger.atInfo().withCause(e).log(re.toString());
                    ctx.status(HttpServletResponse.SC_CONFLICT).json(re);
                })
                .exception(DeleteConflictException.class, (e, ctx) -> {
                    CdaError re = new CdaError("Cannot perform requested delete. Data is referenced elsewhere in CWMS.", e.getDetails());
                    logger.atInfo().withCause(e).log(re.toString(), e);
                    ctx.status(HttpServletResponse.SC_CONFLICT).json(re);
                })
                .exception(NotFoundException.class, (e, ctx) -> {
                    CdaError re = new CdaError("Not Found.");
                    logger.atInfo().withCause(e).log(re.toString());
                    ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
                })
                .exception(FieldException.class, (e, ctx) -> {
                    CdaError re = new CdaError(e.getMessage(), e.getDetails(), true);
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(DateTimeException.class, (e, ctx) -> {
                    CdaError re = new CdaError(e.getMessage());
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(JsonFieldsException.class, (e, ctx) -> {
                    CdaError re = new CdaError(e.getMessage(), e.getDetails(), true);
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(CwmsAuthException.class, (e,ctx) -> {
                    CdaError re;
                    switch (e.getAuthFailCode()) {
                        case 401: re = new CdaError("Invalid user",true); break;
                        case 403: re = new CdaError("Not Authorized",true); break;
                        default: re = new CdaError("Unknown auth error.");
                    }

                    if (logger.atFine().isEnabled()) {
                        logger.atFine().withCause(e).log(e.getMessage());
                    } else {
                        logger.atInfo().log(e.getMessage());
                    }

                    ctx.status(e.getAuthFailCode()).json(re);
                })
                .exception(Exception.class, (e, ctx) -> {
                    CdaError errResponse = new CdaError("System Error");
                    logger.atWarning().withCause(e).log("error on request[%s]: %s",
                            errResponse.getIncidentIdentifier(), ctx.req.getRequestURI());
                    ctx.status(500);
                    ctx.contentType(ContentType.APPLICATION_JSON.toString());
                    ctx.json(errResponse);
                })
                .routes(this::configureRoutes);
    }

    private List<String> getExternalRootDirs() {
        List<String> externalRootDir = new ArrayList<>();

        for (int i = 1; i < 100; i++) {
            String key = "cwms.dataapi.external.root.dir." + i;
            String dir = System.getProperty(key);
            if (dir != null) {
                externalRootDir.add(dir);
            } else {
                break;
            }
        }

        return externalRootDir;
    }

    private String obtainFullVersion(ServletConfig servletConfig) throws ServletException {
        String relativeWARPath = "/META-INF/MANIFEST.MF";
        String absoluteDiskPath = servletConfig.getServletContext().getRealPath(relativeWARPath);
        if(absoluteDiskPath == null) {
            return "servlet context getRealPath returned null";
        }
        logger.atInfo().log("Absolute path to manifest is: %s", absoluteDiskPath);
        Path path = Paths.get(absoluteDiskPath);

        try (InputStream inputStream = Files.newInputStream(path)) {
            Manifest manifest = new Manifest(inputStream);
            return manifest.getMainAttributes().getValue("build-version");
        } catch (IOException e) {
            throw new ServletException("Error obtaining servlet version", e);
        }
    }

    private CdaAccessManager buildAccessManager(String provider) {
        try {
            AccessManagers ams = new AccessManagers();
            return ams.get(provider);
        } catch (ServiceNotFoundException err) {
            throw new RuntimeException("Unable to initialize access manager",err);
        }

    }

    protected void configureRoutes() {

        RouteRole[] requiredRoles = {new Role(CWMS_USERS_ROLE)};

        String welcomeHtml = "<html><head><title>CWMS Data API</title></head><body>"
                + "<h1>CWMS Data API</h1>"
                + "<p>Welcome to the CWMS Data API.  You might be interested in: "
                + "<ul>"
                + "<li><a href=\"swagger-ui.html\">Swagger UI</a></li>"
                + "<li><a href=\"swagger-docs\">Swagger Docs</a></li>"
                + "<li><a href=\"offices\">Offices</a></li>"
                + "<li><a href=\"ws_demo.html\">websocket clob demo</a></li>"
                + "<li><a href=\"sse_demo.html\">sse clob demo</a></li>"
                + "</ul>"
                + "</body></html>";

        get("/", ctx -> ctx.result(welcomeHtml)
                .contentType("text/html"));
        crud("/auth/keys/{key-name}",new ApiKeyController(metrics), requiredRoles);
        cdaCrudCache("/location/category/{category-id}",
                new LocationCategoryController(metrics), requiredRoles, 5, TimeUnit.MINUTES);
        cdaCrudCache("/location/group/{group-id}",
                new LocationGroupController(metrics), requiredRoles, 5, TimeUnit.MINUTES);
        cdaCrudCache("/locations/{location-id}",
                new LocationController(metrics), requiredRoles, 5, TimeUnit.MINUTES);
        cdaCrudCache("/states/{state}",
                new StateController(metrics), requiredRoles, 60, TimeUnit.MINUTES);
        cdaCrudCache("/counties/{county}",
                new CountyController(metrics), requiredRoles, 60, TimeUnit.MINUTES);
        cdaCrudCache("/offices/{office}",
                new OfficeController(metrics), requiredRoles, 60, TimeUnit.MINUTES);
        cdaCrudCache("/units/{unit-id}",
                new UnitsController(metrics), requiredRoles, 60, TimeUnit.MINUTES);
        cdaCrudCache("/parameters/{param-id}",
                new ParametersController(metrics), requiredRoles, 60, TimeUnit.MINUTES);
        cdaCrudCache("/timezones/{zone}",
                new TimeZoneController(metrics), requiredRoles,60, TimeUnit.MINUTES);
        LevelsController levelsController = new LevelsController(metrics);
        cdaCrudCache("/levels/{" + Controllers.LEVEL_ID + "}",
                levelsController, requiredRoles,5, TimeUnit.MINUTES);
        String levelTsPath = "/levels/{" + Controllers.LEVEL_ID + "}/timeseries";
        get(levelTsPath, levelsController::getLevelAsTimeSeries);
        addCacheControl(levelTsPath, 5, TimeUnit.MINUTES);
        TimeSeriesController tsController = new TimeSeriesController(metrics);
        String recentPath = "/timeseries/recent/{group-id}";
        get(recentPath, tsController::getRecent);
        addCacheControl(recentPath, 5, TimeUnit.MINUTES);

        cdaCrudCache("/timeseries/category/{category-id}",
                new TimeSeriesCategoryController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/timeseries/identifier-descriptor/{timeseries-id}",
                new TimeSeriesIdentifierDescriptorController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/timeseries/group/{group-id}",
                new TimeSeriesGroupController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/timeseries/{timeseries}", tsController, requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/ratings/template/{template-id}",
                new RatingTemplateController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/ratings/spec/{rating-id}",
                new RatingSpecController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/ratings/metadata/{rating-id}",
                new RatingMetadataController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/ratings/{rating-id}",
                new RatingController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/catalog/{dataset}",
                new CatalogController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/basins/{basin-id}",
                new BasinController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/blobs/{blob-id}",
                new BlobController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/clobs/{clob-id}",
                new ClobController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/pools/{pool-id}",
                new PoolController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/specified-levels/{specified-level-id}",
                new SpecifiedLevelController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrud("/ping/{ping-id}",
                new PingController(), requiredRoles);

        ClobAsyncController clobSSEController = new ClobAsyncController();

        sse("/sse/clob", clobSSEController.getSseConsumer());


        ws("/ws/clob", clobSSEController.getWsConsumer(), new RouteRole[0]);
        ws("/ws/clob2", clobSSEController.getWsConsumer2(), new RouteRole[0]);

    }



    /**
     * This method delegates to the cdaCrud method but also adds an after filter for the specified
     * path.  If the request was a GET request and the response does not already include
     * Cache-Control then the filter will add the Cache-Control max-age header with the specified
     * number of seconds.
     * Controllers can include their own Cache-Control headers via:
     *  "ctx.header(Header.CACHE_CONTROL, " public, max-age=" + 60);"
     * This method lets the ApiServlet configure a default max-age for controllers that don't or
     * forget to set their own.
     * @param path where to register the routes.
     * @param crudHandler the handler requests should be forwarded to.
     * @param roles the required these roles are present to access post, patch
     * @param duration the number of TimeUnit to cache GET responses.
     * @param timeUnit the TimeUnit to use for duration.
     */
    public static void cdaCrudCache(@NotNull String path, @NotNull CrudHandler crudHandler,
                                    @NotNull RouteRole[] roles, long duration, TimeUnit timeUnit) {
        cdaCrud(path, crudHandler, roles);

        // path like /offices/{office} will match /offices/SWT getOne style url
        addCacheControl(path, duration, timeUnit);

        String pathWithoutResource = path.replace(getResourceId(path), "");
        // path like "/offices/" matches /offices getAll style url
        addCacheControl(pathWithoutResource, duration, timeUnit);
    }

    private static void addCacheControl(@NotNull String path, long duration, TimeUnit timeUnit) {
        if(timeUnit != null && duration > 0) {
            staticInstance().after(path, ctx -> {
                String method = ctx.req.getMethod();  // "GET"
                if (ctx.status() == HttpServletResponse.SC_OK
                        && "GET".equals(method)
                        && (!ctx.res.containsHeader(Header.CACHE_CONTROL))) {
                    // only set the cache control header if it is not already set.
                    ctx.header(Header.CACHE_CONTROL, "max-age=" + timeUnit.toSeconds(duration));
                }
            });
        }
    }

    /**
     * This method is very similar to the ApiBuilder.crud method but the specified roles
     * are only required for the post, patch and delete methods.  getOne and getAll are always
     * allowed.
     * @param path where to register the routes.
     * @param crudHandler the handler requests should be forwarded to.
     * @param roles the accessmanager will require these roles are present to access post, patch
     *             and delete methods
     */
    public static void cdaCrud(@NotNull String path, @NotNull CrudHandler crudHandler,
                                 @NotNull RouteRole... roles) {
        String fullPath = prefixPath(path);
        String resourceId = getResourceId(fullPath);

        //noinspection KotlinInternalInJava
        Map<CrudFunction, Handler> crudFunctions =
                CrudHandlerKt.getCrudFunctions(crudHandler, resourceId);

        Javalin instance = staticInstance();
        // getOne and getAll are assumed not to need authorization
        instance.get(fullPath, crudFunctions.get(CrudFunction.GET_ONE));
        String pathWithoutResource = fullPath.replace(resourceId, "");
        instance.get(pathWithoutResource,
                crudFunctions.get(CrudFunction.GET_ALL));

        // create, update and delete need authorization.
        instance.post(pathWithoutResource,
                crudFunctions.get(CrudFunction.CREATE), roles);
        instance.patch(fullPath, crudFunctions.get(CrudFunction.UPDATE), roles);
        instance.delete(fullPath, crudFunctions.get(CrudFunction.DELETE), roles);
    }


    /**
     * Given a path like "/location/category/{category-id}" this method returns "{category-id}"
     * @param fullPath
     * @return
     */
    @NotNull
    public static String getResourceId(String fullPath) {
        String[] subPaths = Arrays.stream(fullPath.split("/"))
                .filter(it -> !it.isEmpty()).toArray(String[]::new);
        if (subPaths.length < 2) {
            throw new IllegalArgumentException("CrudHandler requires a path like "
                    + "'/resource/{resource-id}' given: " + fullPath);
        }
        String resourceId = subPaths[subPaths.length - 1];
        if (!(
                (resourceId.startsWith("{") && resourceId.endsWith("}"))
                ||
                (resourceId.startsWith("<") && resourceId.endsWith(">"))
            )) {
            throw new IllegalArgumentException("CrudHandler requires a path-parameter at the "
                    + "end of the provided path, e.g. '/users/{user-id}' or '/users/<user-id>' given: " + fullPath);
        }
        String resourceBase = subPaths[subPaths.length - 2];
        if (resourceBase.startsWith("{") || resourceBase.startsWith("<")
                || resourceBase.endsWith("}") || resourceBase.endsWith(">")) {
            throw new IllegalArgumentException("CrudHandler requires a resource base at the "
                    + "beginning of the provided path, e.g. '/users/{user-id}' given: " + fullPath);
        }
        return resourceId;
    }

    private void getOpenApiOptions(JavalinConfig config) {
        Info applicationInfo = new Info().title(APPLICATION_TITLE).version(VERSION)
                .description("CWMS REST API for Data Retrieval");

        String provider = getAccessManagerName();

        CdaAccessManager am = buildAccessManager(provider);
        Components components = new Components();
        final ArrayList<SecurityRequirement> secReqs = new ArrayList<>();
        am.getContainedManagers().forEach(manager -> {
            components.addSecuritySchemes(manager.getName(),manager.getScheme());
            SecurityRequirement req = new SecurityRequirement();
            if (!manager.getName().equalsIgnoreCase("guestauth") && !manager.getName().equalsIgnoreCase("noauth")) {
                req.addList(manager.getName());
                secReqs.add(req);
            }
        });

        config.accessManager(am);

        OpenApiOptions ops =
            new OpenApiOptions(
                () -> new OpenAPI().components(components)
                                   .info(applicationInfo)
                                   .addSecurityItem(new SecurityRequirement().addList(provider))
        );
        ops.path("/swagger-docs")
            .responseModifier((ctx,api) -> {
                api.getPaths().forEach((key,path) -> setSecurityRequirements(key,path,secReqs));
                return api;
            })
            .defaultDocumentation(doc -> {
                doc.json("500", CdaError.class);
                doc.json("400", CdaError.class);
                doc.json("401", CdaError.class);
                doc.json("403", CdaError.class);
                doc.json("404", CdaError.class);
            })
            .activateAnnotationScanningFor("cwms.cda.api");

        config.registerPlugin(new OpenApiPlugin(ops));

    }

    private static void setSecurityRequirements(String key, PathItem path,List<SecurityRequirement> secReqs) {
        /* clear the lock icon from the GET handlers to reduce user confusion */
        logger.atFinest().log("setting security constraints for " + key);
        if (key.contains("/auth/")) {
            setSecurity(path.getGet(), secReqs);
        } else {
            setSecurity(path.getGet(), new ArrayList<>());
        }
        setSecurity(path.getDelete(),secReqs);
        setSecurity(path.getPost(), secReqs);
        setSecurity(path.getPut(), secReqs);
        setSecurity(path.getPatch(),secReqs);
    }

    private static void setSecurity(Operation op,List<SecurityRequirement> reqs) {
        if (op != null) {
            op.setSecurity(reqs);
        }
    }

    private static String getAccessManagerName() {
        // Default to CwmsAccessManager
        String defProvider = DEFAULT_PROVIDER;

        // If something is set in the environment, make that the new default.
        // This is useful because Docker makes it easy to set environment variables.
        String envProvider = System.getenv(PROVIDER_KEY_OLD);
        if( envProvider == null) {
            envProvider = System.getenv(PROVIDER_KEY);
        }
        if (envProvider != null) {
            defProvider = envProvider;
        }

        // Return the value from properties or the default
        return System.getProperty(PROVIDER_KEY, System.getProperty(PROVIDER_KEY_OLD,defProvider));
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        totalRequests.mark();
        try {
            String office = officeFromContext(req.getContextPath());
            req.setAttribute(OFFICE_ID, office);
            //logger.atInfo().log("Connection user name is: %s")
            req.setAttribute(DATA_SOURCE, cwms);
            req.setAttribute(RAW_DATA_SOURCE,cwms);
            javalinServlet.service(req, resp);
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

    public static String officeFromContext(String contextPath) {
        String office = contextPath.split("-")[0].replaceFirst("/","");
        if (office.isEmpty() || office.equalsIgnoreCase("cwms")) {
            office = "HQ";
        }
        return System.getProperty(DEFAULT_OFFICE_KEY, office).toUpperCase();
    }

    public static void main(String[] args) throws ServletException, SQLException {
        Log.getLog().setDebugEnabled(false);
        ApiServlet servlet = new ApiServlet();
        servlet.setDataSource(buildDataSource());  // running from main doesn't do @Resource injection
        servlet.init();

        servlet.javalin.start(7000);
    }

    private static void manualJetty() throws Exception {
        Log.setLog(new StdErrLog());
        Log.getLog().setDebugEnabled(false);

        Server server = new Server(new QueuedThreadPool(100, 10, 120));
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(7000);
        server.setConnectors(new Connector[] { connector });

        String webAppFolderPath = "J:\\Workspaces\\git\\cwms-radar-api\\cwms-data-api\\build\\libs\\cwms-data-api-3.1.0.war" ;
        org.eclipse.jetty.server.Handler webAppHandler = new WebAppContext(webAppFolderPath, "/cwms-data");
        server.setHandler(webAppHandler);


        server.start();
    }

    private static DataSource buildDataSource() throws SQLException {
        OracleDataSource ods = new OracleDataSource();

        String cdaJdbcUrl = System.getenv("CDA_JDBC_URL");
        if (cdaJdbcUrl == null){
            cdaJdbcUrl = System.getProperty("CDA_JDBC_URL");
        }
        System.out.println("Connecting to: " + cdaJdbcUrl);
        ods.setURL(cdaJdbcUrl);

        String username = System.getenv("CDA_JDBC_USERNAME");
        if(username == null){
            username = System.getProperty("CDA_JDBC_USERNAME");
        }
        ods.setUser(username);

        String password = System.getenv("CDA_JDBC_PASSWORD");
        if(password == null){
            password = System.getProperty("CDA_JDBC_PASSWORD");
        }
        ods.setPassword(password);



        return ods;
    }


    public void setDataSource(DataSource dataSource) {
        this.cwms = dataSource;
    }
}
