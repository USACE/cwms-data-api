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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.flogger.FluentLogger;

import cwms.cda.spi.AccessManagers;
import cwms.cda.spi.CdaAccessManager;
import cwms.cda.api.BasinController;
import cwms.cda.api.BlobController;
import cwms.cda.api.CatalogController;
import cwms.cda.api.ClobController;
import cwms.cda.api.Controllers;
import cwms.cda.api.LevelsController;
import cwms.cda.api.LocationCategoryController;
import cwms.cda.api.LocationController;
import cwms.cda.api.LocationGroupController;
import cwms.cda.api.OfficeController;
import cwms.cda.api.ParametersController;
import cwms.cda.api.PoolController;
import cwms.cda.api.RatingController;
import cwms.cda.api.RatingMetadataController;
import cwms.cda.api.RatingSpecController;
import cwms.cda.api.RatingTemplateController;
import cwms.cda.api.SpecifiedLevelController;
import cwms.cda.api.TimeSeriesCategoryController;
import cwms.cda.api.TimeSeriesController;
import cwms.cda.api.TimeSeriesGroupController;
import cwms.cda.api.TimeSeriesIdentifierDescriptorController;
import cwms.cda.api.TimeZoneController;
import cwms.cda.api.UnitsController;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.*;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.security.CwmsAuthException;
import cwms.cda.security.Role;
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
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import org.apache.http.entity.ContentType;
import org.jetbrains.annotations.NotNull;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;

import javax.annotation.Resource;
import javax.management.ServiceNotFoundException;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.javalin.apibuilder.ApiBuilder.*;


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
    public static final String RAW_DATA_SOURCE = "data_source";
    public static final String DATABASE = "database";

    // The VERSION should match the gradle version but not contain the patch version.
    // For example 2.4 not 2.4.13
    public static final String VERSION = "3.0";
    public static final String PROVIDER_KEY_OLD = "radar.access.provider";
    public static final String PROVIDER_KEY = "cwms.dataapi.access.provider";
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
                    logger.atInfo().withCause(e).log(re.toString(), e);
                    ctx.status(e.getStatus()).json(re);
                })
                .exception(IllegalArgumentException.class, (e, ctx) -> {
                    CdaError re = new CdaError("Bad Request");
                    logger.atInfo().withCause(e).log(re.toString(), e);
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(InvalidItemException.class, (e, ctx) -> {
                    CdaError re = new CdaError("Bad Request.");
                    logger.atInfo().withCause(e).log(re.toString(), e);
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(AlreadyExists.class, (e, ctx) -> {
                    CdaError re = new CdaError("Already Exists.");
                    logger.atInfo().withCause(e).log(re.toString(), e);
                    ctx.status(HttpServletResponse.SC_CONFLICT).json(re);
                })
                .exception(DeleteConflictException.class, (e, ctx) -> {
                    CdaError re = new CdaError("Cannot perform requested delete. Data is referenced elsewhere in CWMS.", e.getDetails());
                    logger.atInfo().withCause(e).log(re.toString(), e);
                    ctx.status(HttpServletResponse.SC_CONFLICT).json(re);
                })
                .exception(NotFoundException.class, (e, ctx) -> {
                    CdaError re = new CdaError("Not Found.");
                    logger.atInfo().withCause(e).log(re.toString(), e);
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
                    CdaError re = new CdaError(e.getMessage(),true);
                    logger.atInfo().withCause(e).log("Failed Authorization");
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

                .routes(this::configureRoutes)
                .javalinServlet();        
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

        get("/", ctx -> ctx.result("Welcome to the CWMS REST API")
                .contentType(Formats.PLAIN));
        cdaCrud("/location/category/{category-id}",
                new LocationCategoryController(metrics), requiredRoles);
        cdaCrud("/location/group/{group-id}",
                new LocationGroupController(metrics), requiredRoles);
        cdaCrud("/locations/{location-id}",
                new LocationController(metrics), requiredRoles);
        cdaCrud("/offices/{office}",
                new OfficeController(metrics), requiredRoles);
        cdaCrud("/units/{unit-id}",
                new UnitsController(metrics), requiredRoles);
        cdaCrud("/parameters/{param-id}",
                new ParametersController(metrics), requiredRoles);
        cdaCrud("/timezones/{zone}",
                new TimeZoneController(metrics), requiredRoles);
        cdaCrud("/levels/{" + Controllers.LEVEL_ID + "}",
                new LevelsController(metrics), requiredRoles);
        TimeSeriesController tsController = new TimeSeriesController(metrics);
        get("/timeseries/recent/{group-id}", tsController::getRecent);
        cdaCrud("/timeseries/category/{category-id}",
                new TimeSeriesCategoryController(metrics), requiredRoles);
        cdaCrud("/timeseries/identifier-descriptor/{timeseries-id}",
                new TimeSeriesIdentifierDescriptorController(metrics), requiredRoles);
        cdaCrud("/timeseries/group/{group-id}",
                new TimeSeriesGroupController(metrics), requiredRoles);
        cdaCrud("/timeseries/{timeseries}", tsController, requiredRoles);
        cdaCrud("/ratings/template/{template-id}",
                new RatingTemplateController(metrics), requiredRoles);
        cdaCrud("/ratings/spec/{rating-id}",
                new RatingSpecController(metrics), requiredRoles);
        cdaCrud("/ratings/metadata/{rating-id}",
                new RatingMetadataController(metrics), requiredRoles);
        cdaCrud("/ratings/{rating-id}",
                new RatingController(metrics), requiredRoles);
        cdaCrud("/catalog/{dataset}",
                new CatalogController(metrics), requiredRoles);
        cdaCrud("/basins/{basin-id}",
                new BasinController(metrics), requiredRoles);
        cdaCrud("/blobs/{blob-id}",
                new BlobController(metrics), requiredRoles);
        cdaCrud("/clobs/{clob-id}",
                new ClobController(metrics), requiredRoles);
        cdaCrud("/pools/{pool-id}",
                new PoolController(metrics), requiredRoles);
        cdaCrud("/specified-levels/{specified-level-id}",
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
    public static void cdaCrud(@NotNull String path, @NotNull CrudHandler crudHandler,
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
        Info applicationInfo = new Info().title("CWMS Data API").version(VERSION)
                .description("CWMS REST API for Data Retrieval");

        String provider = getAccessManagerName();
        logger.atInfo().log("Using access provider:" + provider);

        CdaAccessManager am = buildAccessManager(provider);
        Components components = new Components();
        final ArrayList<SecurityRequirement> secReqs = new ArrayList<>();
        am.getContainedManagers().forEach((manager)->{
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
                api.getPaths().forEach((key,path) -> {
                    setSecurityRequirements(key,path,secReqs);
                });
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
        Operation op = path.getGet();
        logger.atFinest().log("setting security constraints for " + key);
        setSecurity(path.getGet(), new ArrayList<>());
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

    public static String officeFromContext(String contextPath) {
        String office = contextPath.split("-")[0].replaceFirst("/","");
        if (office.isEmpty() || office.equalsIgnoreCase("cwms")) {
            office = "HQ";
        }
        return office.toUpperCase();
    }

}
