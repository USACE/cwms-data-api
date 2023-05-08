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

package cwms.radar;

import static io.javalin.apibuilder.ApiBuilder.get;
import static io.javalin.apibuilder.ApiBuilder.prefixPath;
import static io.javalin.apibuilder.ApiBuilder.staticInstance;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.flogger.FluentLogger;
import cwms.radar.api.BasinController;
import cwms.radar.api.BlobController;
import cwms.radar.api.CatalogController;
import cwms.radar.api.ClobController;
import cwms.radar.api.Controllers;
import cwms.radar.api.LevelsController;
import cwms.radar.api.LocationCategoryController;
import cwms.radar.api.LocationController;
import cwms.radar.api.LocationGroupController;
import cwms.radar.api.OfficeController;
import cwms.radar.api.ParametersController;
import cwms.radar.api.PoolController;
import cwms.radar.api.RatingController;
import cwms.radar.api.RatingMetadataController;
import cwms.radar.api.RatingSpecController;
import cwms.radar.api.RatingTemplateController;
import cwms.radar.api.SpecifiedLevelController;
import cwms.radar.api.TimeSeriesCategoryController;
import cwms.radar.api.TimeSeriesController;
import cwms.radar.api.TimeSeriesGroupController;
import cwms.radar.api.TimeSeriesIdentifierDescriptorController;
import cwms.radar.api.TimeZoneController;
import cwms.radar.api.UnitsController;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.api.errors.FieldException;
import cwms.radar.api.errors.InvalidItemException;
import cwms.radar.api.errors.JsonFieldsException;
import cwms.radar.api.errors.NotFoundException;
import cwms.radar.api.errors.RadarError;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import cwms.radar.security.CwmsAuthException;
import cwms.radar.security.Role;
import cwms.radar.spi.AccessManagers;
import cwms.radar.spi.RadarAccessManager;
import io.javalin.Javalin;
import io.javalin.apibuilder.CrudFunction;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.apibuilder.CrudHandlerKt;
import io.javalin.core.JavalinConfig;
import io.javalin.core.security.RouteRole;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.http.BadRequestResponse;
import io.javalin.http.Handler;
import io.javalin.http.JavalinServlet;
import io.javalin.plugin.openapi.OpenApiOptions;
import io.javalin.plugin.openapi.OpenApiPlugin;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Resource;
import javax.management.ServiceNotFoundException;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import org.apache.http.entity.ContentType;
import org.jetbrains.annotations.NotNull;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;


/**
 * Setup all the information required so we can serve the request.
 *
 */
@WebServlet(urlPatterns = { "/catalog/*",
        "/swagger-docs",
        "/timeseries/*",
        "/offices/*",
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
        "/specified-levels/*"
})
public class ApiServlet extends HttpServlet {

    public static final FluentLogger logger = FluentLogger.forEnclosingClass();

    // based on https://bitbucket.hecdev.net/projects/CWMS/repos/cwms_aaa/browse/IntegrationTests/src/test/resources/sql/load_testusers.sql
    public static final String CWMS_USERS_ROLE = "CWMS Users";
    public static final String OFFICE_ID = "office_id";
    public static final String DATA_SOURCE = "data_source";
    public static final String DATABASE = "database";

    // The VERSION should match the gradle version but not contain the patch version.
    // For example 2.4 not 2.4.13
    public static final String VERSION = "3.0";
    public static final String PROVIDER_KEY = "radar.access.provider";
    public static final String DEFAULT_PROVIDER = "MultipleAccessManager";

    private MetricRegistry metrics;
    private Meter totalRequests;

    private static final long serialVersionUID = 1L;

    JavalinServlet javalin = null;

    @Resource(name = "jdbc/CWMS3")
    DataSource cwms;



    @Override
    public void destroy() {
        javalin.destroy();
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        metrics = (MetricRegistry)config.getServletContext()
                .getAttribute(MetricsServlet.METRICS_REGISTRY);
        totalRequests = metrics.meter("radar.total_requests");
        super.init(config);
    }

    @SuppressWarnings({"java:S125","java:S2095"}) // closed in destroy handler
    @Override
    public void init() {
        logger.atInfo().log("Initializing API");
        JavalinValidation.register(UnitSystem.class, UnitSystem::systemFor);
        ObjectMapper om = new ObjectMapper();
        om.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        om.registerModule(new JavaTimeModule());

        PolicyFactory sanitizer = new HtmlPolicyBuilder().disallowElements("<script>").toFactory();
        String context = this.getServletContext().getContextPath();
        javalin = Javalin.createStandalone(config -> {
                    config.defaultContentType = "application/json";
                    config.contextPath = context;
                    getOpenApiOptions(config);
                    //config.registerPlugin(new OpenApiPlugin());
                    //config.enableDevLogging();
                    config.requestLogger((ctx, ms) -> logger.atFinest().log(ctx.toString()));
                })
                .attribute("PolicyFactory", sanitizer)
                .attribute("ObjectMapper", om)
                .before(ctx -> {
                    ctx.attribute("sanitizer", sanitizer);
                    ctx.header("X-Content-Type-Options", "nosniff");
                    ctx.header("X-Frame-Options", "SAMEORIGIN");
                    ctx.header("X-XSS-Protection", "1; mode=block");
                })
                .exception(FormattingException.class, (fe, ctx) -> {
                    final RadarError re = new RadarError("Formatting error:" + fe.getMessage());

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
                    final RadarError re = RadarError.notImplemented();
                    logger.atWarning().withCause(e)
                            .log("%s for request: %s", re, ctx.fullUrl());
                    ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(re);
                })
                .exception(BadRequestResponse.class, (e, ctx) -> {
                    RadarError re = new RadarError("Bad Request", e.getDetails());
                    logger.atInfo().withCause(e).log(re.toString(), e);
                    ctx.status(e.getStatus()).json(re);
                })
                .exception(IllegalArgumentException.class, (e, ctx) -> {
                    RadarError re = new RadarError("Bad Request");
                    logger.atInfo().withCause(e).log(re.toString(), e);
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(InvalidItemException.class, (e, ctx) -> {
                    RadarError re = new RadarError("Bad Request.");
                    logger.atInfo().withCause(e).log(re.toString(), e);
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(NotFoundException.class, (e, ctx) -> {
                    RadarError re = new RadarError("Not Found.");
                    logger.atInfo().withCause(e).log(re.toString(), e);
                    ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
                })
                .exception(FieldException.class, (e, ctx) -> {
                    RadarError re = new RadarError(e.getMessage(), e.getDetails(), true);
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(DateTimeException.class, (e, ctx) -> {
                    RadarError re = new RadarError(e.getMessage());
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(JsonFieldsException.class, (e, ctx) -> {
                    RadarError re = new RadarError(e.getMessage(), e.getDetails(), true);
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(CwmsAuthException.class, (e,ctx) -> {
                    RadarError re = new RadarError(e.getMessage(),true);
                    ctx.status(HttpServletResponse.SC_UNAUTHORIZED).json(re);
                })
                .exception(Exception.class, (e, ctx) -> {
                    RadarError errResponse = new RadarError("System Error");
                    logger.atWarning().withCause(e).log("error on request[%s]: %s",
                            errResponse.getIncidentIdentifier(), ctx.req.getRequestURI());
                    ctx.status(500);
                    ctx.contentType(ContentType.APPLICATION_JSON.toString());
                    ctx.json(errResponse);
                })

                .routes(this::configureRoutes)
                .javalinServlet();        
    }

    private RadarAccessManager buildAccessManager(String provider) {
        try {            
            AccessManagers ams = new AccessManagers();       
            return ams.get(provider);
        } catch (ServiceNotFoundException err) {
            throw new RuntimeException("Unable to initialize access manager",err);
        }
        
    }




    protected void configureRoutes() {

        RouteRole[] requiredRoles = {new Role(CWMS_USERS_ROLE)};

        get("/", ctx -> ctx.result("Welcome to the CWMS REST API")
                .contentType(Formats.PLAIN));
        radarCrud("/location/category/{category-id}",
                new LocationCategoryController(metrics), requiredRoles);
        radarCrud("/location/group/{group-id}",
                new LocationGroupController(metrics), requiredRoles);
        radarCrud("/locations/{location-id}",
                new LocationController(metrics), requiredRoles);
        radarCrud("/offices/{office}",
                new OfficeController(metrics), requiredRoles);
        radarCrud("/units/{unit-id}",
                new UnitsController(metrics), requiredRoles);
        radarCrud("/parameters/{param-id}",
                new ParametersController(metrics), requiredRoles);
        radarCrud("/timezones/{zone}",
                new TimeZoneController(metrics), requiredRoles);
        radarCrud("/levels/{" + Controllers.LEVEL_ID + "}",
                new LevelsController(metrics), requiredRoles);
        TimeSeriesController tsController = new TimeSeriesController(metrics);
        get("/timeseries/recent/{group-id}", tsController::getRecent);
        radarCrud("/timeseries/category/{category-id}",
                new TimeSeriesCategoryController(metrics), requiredRoles);
        radarCrud("/timeseries/identifier-descriptor/{timeseries-id}",
                new TimeSeriesIdentifierDescriptorController(metrics), requiredRoles);
        radarCrud("/timeseries/group/{group-id}",
                new TimeSeriesGroupController(metrics), requiredRoles);
        radarCrud("/timeseries/{timeseries}", tsController, requiredRoles);
        radarCrud("/ratings/template/{template-id}",
                new RatingTemplateController(metrics), requiredRoles);
        radarCrud("/ratings/spec/{rating-id}",
                new RatingSpecController(metrics), requiredRoles);
        radarCrud("/ratings/metadata/{rating-id}",
                new RatingMetadataController(metrics), requiredRoles);
        radarCrud("/ratings/{rating}",
                new RatingController(metrics), requiredRoles);
        radarCrud("/catalog/{dataset}",
                new CatalogController(metrics), requiredRoles);
        radarCrud("/basins/{basin-id}",
                new BasinController(metrics), requiredRoles);
        radarCrud("/blobs/{blob-id}",
                new BlobController(metrics), requiredRoles);
        radarCrud("/clobs/{clob-id}",
                new ClobController(metrics), requiredRoles);
        radarCrud("/pools/{pool-id}",
                new PoolController(metrics), requiredRoles);
        radarCrud("/specified-levels/{level-id}",
                new SpecifiedLevelController(metrics), requiredRoles);
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
    public static void radarCrud(@NotNull String path, @NotNull CrudHandler crudHandler,
                                 @NotNull RouteRole... roles) {
        String fullPath = prefixPath(path);
        String[] subPaths = Arrays.stream(fullPath.split("/"))
                .filter(it -> !it.isEmpty()).toArray(String[]::new);
        if (subPaths.length < 2) {
            throw new IllegalArgumentException("CrudHandler requires a path like "
                    + "'/resource/{resource-id}'");
        }
        String resourceId = subPaths[subPaths.length - 1];
        if (!(
                (resourceId.startsWith("{") && resourceId.endsWith("}"))
                ||
                (resourceId.startsWith("<") && resourceId.endsWith(">"))
            )) {
            throw new IllegalArgumentException("CrudHandler requires a path-parameter at the "
                    + "end of the provided path, e.g. '/users/{user-id}' or '/users/<user-id>'");
        }
        String resourceBase = subPaths[subPaths.length - 2];
        if (resourceBase.startsWith("{") || resourceBase.startsWith("<")
                || resourceBase.endsWith("}") || resourceBase.endsWith(">")) {
            throw new IllegalArgumentException("CrudHandler requires a resource base at the "
                    + "beginning of the provided path, e.g. '/users/{user-id}'");
        }

        //noinspection KotlinInternalInJava
        Map<CrudFunction, Handler> crudFunctions =
                CrudHandlerKt.getCrudFunctions(crudHandler, resourceId);

        Javalin instance = staticInstance();
        // getOne and getAll are assumed not to need authorization
        instance.get(fullPath, crudFunctions.get(CrudFunction.GET_ONE));
        instance.get(fullPath.replace(resourceId, ""),
                crudFunctions.get(CrudFunction.GET_ALL));

        // create, update and delete need authorization.
        instance.post(fullPath.replace(resourceId, ""),
                crudFunctions.get(CrudFunction.CREATE), roles);
        instance.patch(fullPath, crudFunctions.get(CrudFunction.UPDATE), roles);
        instance.delete(fullPath, crudFunctions.get(CrudFunction.DELETE), roles);
    }

    private void getOpenApiOptions(JavalinConfig config) {
        Info applicationInfo = new Info().title("CWMS Radar").version(VERSION)
                .description("CWMS REST API for Data Retrieval");

        String provider = getAccessManagerName();
        logger.atInfo().log("Using access provider:" + provider);

        RadarAccessManager am = buildAccessManager(provider);
        Components components = new Components();
        am.getContainedManagers().forEach((manager)->{
            components.addSecuritySchemes(manager.getName(),manager.getScheme());
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
                api.getPaths().forEach((key,path) -> {
                    /* clear the lock icon from the GET handlers to reduce user confusion */
                    Operation op = path.getGet();
                    if (op != null) {
                        logger.atInfo().log("removing security constraint for GET on " + key);
                        op.setSecurity(new ArrayList<>());
                    }
                });                    
                return api;
            })
            .defaultDocumentation(doc -> {
                doc.json("500", RadarError.class);
                doc.json("400", RadarError.class);
                doc.json("401", RadarError.class);
                doc.json("403", RadarError.class);
                doc.json("404", RadarError.class);                
            })
            .activateAnnotationScanningFor("cwms.radar.api");
        config.registerPlugin(new OpenApiPlugin(ops));
        
    }

    private static String getAccessManagerName() {
        // Default to CwmsAccessManager
        String defProvider = DEFAULT_PROVIDER;

        // If something is set in the environment, make that the new default.
        // This is useful because Docker makes it easy to set environment variables.
        String envProvider = System.getenv(PROVIDER_KEY);
        if (envProvider != null) {
            defProvider = envProvider;
        }

        // Return the value from properties or the default
        return System.getProperty(PROVIDER_KEY, defProvider);
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        totalRequests.mark();
        try {
            String office = officeFromContext(req.getContextPath());
            req.setAttribute(OFFICE_ID, office);

            req.setAttribute(DATA_SOURCE, cwms);
            javalin.service(req, resp);
        } catch (Exception ex) {
            RadarError re = new RadarError("Major Database Issue");
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
        return office.toUpperCase();
    }

}
