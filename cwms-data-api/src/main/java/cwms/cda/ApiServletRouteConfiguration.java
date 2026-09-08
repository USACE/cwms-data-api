package cwms.cda;

import static cwms.cda.ApiServlet.CAC_USER;
import static cwms.cda.ApiServlet.CWMS_USERS_ROLE;
import static cwms.cda.api.Controllers.CONTRACT_NAME;
import static cwms.cda.api.Controllers.LOCATION_ID;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.api.Controllers.RATING_ID;
import static cwms.cda.api.Controllers.WATER_USER;
import static io.javalin.apibuilder.ApiBuilder.crud;
import static io.javalin.apibuilder.ApiBuilder.delete;
import static io.javalin.apibuilder.ApiBuilder.get;
import static io.javalin.apibuilder.ApiBuilder.patch;
import static io.javalin.apibuilder.ApiBuilder.post;
import static io.javalin.apibuilder.ApiBuilder.prefixPath;
import static io.javalin.apibuilder.ApiBuilder.staticInstance;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.api.BasinController;
import cwms.cda.api.BinaryTimeSeriesController;
import cwms.cda.api.BinaryTimeSeriesValueController;
import cwms.cda.api.BlobController;
import cwms.cda.api.CatalogController;
import cwms.cda.api.CdaVersionHandler;
import cwms.cda.api.ClobController;
import cwms.cda.api.Controllers;
import cwms.cda.api.CountyController;
import cwms.cda.api.DownstreamLocationsGetController;
import cwms.cda.api.EmbankmentController;
import cwms.cda.api.EntityController;
import cwms.cda.api.forecast.ForecastFileController;
import cwms.cda.api.forecast.ForecastInstanceController;
import cwms.cda.api.LevelRefsController;
import cwms.cda.api.LevelsAsTimeSeriesController;
import cwms.cda.api.LevelsController;
import cwms.cda.api.LocationCategoryController;
import cwms.cda.api.LocationController;
import cwms.cda.api.LocationGroupController;
import cwms.cda.api.LocationKindController;
import cwms.cda.api.LookupTypeController;
import cwms.cda.api.MeasurementTimeExtentsGetController;
import cwms.cda.api.OfficeController;
import cwms.cda.api.ParametersController;
import cwms.cda.api.PoolController;
import cwms.cda.api.ProjectController;
import cwms.cda.api.PropertyController;
import cwms.cda.api.PublishedController;
import cwms.cda.api.SpecifiedLevelController;
import cwms.cda.api.StandardTextController;
import cwms.cda.api.StateController;
import cwms.cda.api.StreamController;
import cwms.cda.api.StreamLocationController;
import cwms.cda.api.StreamReachController;
import cwms.cda.api.TextTimeSeriesController;
import cwms.cda.api.TextTimeSeriesValueController;
import cwms.cda.api.TimeSeriesCategoryController;
import cwms.cda.api.TimeSeriesController;
import cwms.cda.api.TimeSeriesFilteredController;
import cwms.cda.api.TimeSeriesGroupController;
import cwms.cda.api.TimeSeriesIdentifierDescriptorController;
import cwms.cda.api.TimeSeriesRecentController;
import cwms.cda.api.TimeSeriesVersionsController;
import cwms.cda.api.TimeZoneController;
import cwms.cda.api.TurbineChangesDeleteController;
import cwms.cda.api.TurbineChangesGetController;
import cwms.cda.api.TurbineChangesPostController;
import cwms.cda.api.TurbineController;
import cwms.cda.api.UnitsController;
import cwms.cda.api.UpstreamLocationsGetController;
import cwms.cda.api.VerticalDatumController;
import cwms.cda.api.auth.ApiKeyController;
import cwms.cda.api.auth.userlists.AddUserListMemberController;
import cwms.cda.api.auth.userlists.CreateUserListController;
import cwms.cda.api.auth.userlists.DeleteUserListController;
import cwms.cda.api.auth.userlists.UpdateUserListController;
import cwms.cda.api.auth.userlists.UserListCandidatesController;
import cwms.cda.api.auth.userlists.UserListController;
import cwms.cda.api.auth.userlists.UserListMemberController;
import cwms.cda.api.auth.userlists.UserListMembersController;
import cwms.cda.api.auth.userlists.UserListsController;
import cwms.cda.api.auth.users.UserProfileController;
import cwms.cda.api.auth.users.UsersController;
import cwms.cda.api.auth.users.roles.AddRoleController;
import cwms.cda.api.auth.users.roles.DeleteRolesController;
import cwms.cda.api.auth.users.roles.GetRolesController;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.forecast.ForecastSpecControllerV1;
import cwms.cda.api.forecast.ForecastSpecControllerV2;
import cwms.cda.api.location.kind.GateChangeCreateController;
import cwms.cda.api.location.kind.GateChangeDeleteController;
import cwms.cda.api.location.kind.GateChangeGetAllController;
import cwms.cda.api.location.kind.LockController;
import cwms.cda.api.location.kind.OutletController;
import cwms.cda.api.location.kind.VirtualOutletController;
import cwms.cda.api.location.kind.VirtualOutletCreateController;
import cwms.cda.api.project.LockRevokerRightsCatalog;
import cwms.cda.api.project.ProjectChildLocationHandler;
import cwms.cda.api.project.ProjectLockCatalog;
import cwms.cda.api.project.ProjectLockGetOne;
import cwms.cda.api.project.ProjectLockRelease;
import cwms.cda.api.project.ProjectLockRequest;
import cwms.cda.api.project.ProjectLockRevoke;
import cwms.cda.api.project.ProjectLockRevokeDeny;
import cwms.cda.api.project.ProjectPublishStatusUpdate;
import cwms.cda.api.project.RemoveAllLockRevokerRights;
import cwms.cda.api.project.UpdateLockRevokerRights;
import cwms.cda.api.rating.RateTimeSeriesController;
import cwms.cda.api.rating.RateValuesController;
import cwms.cda.api.rating.RatingController;
import cwms.cda.api.rating.RatingEffectiveDatesController;
import cwms.cda.api.rating.RatingLatestController;
import cwms.cda.api.rating.RatingMetadataController;
import cwms.cda.api.rating.RatingSpecController;
import cwms.cda.api.rating.RatingTemplateController;
import cwms.cda.api.rating.ReverseRateTimeSeriesController;
import cwms.cda.api.rating.ReverseRateValuesController;
import cwms.cda.api.rss.RssHandler;
import cwms.cda.api.timeseriesprofile.TimeSeriesProfileCatalogController;
import cwms.cda.api.timeseriesprofile.TimeSeriesProfileController;
import cwms.cda.api.timeseriesprofile.TimeSeriesProfileCreateController;
import cwms.cda.api.timeseriesprofile.TimeSeriesProfileDeleteController;
import cwms.cda.api.timeseriesprofile.TimeSeriesProfileInstanceCatalogController;
import cwms.cda.api.timeseriesprofile.TimeSeriesProfileInstanceController;
import cwms.cda.api.timeseriesprofile.TimeSeriesProfileInstanceCreateController;
import cwms.cda.api.timeseriesprofile.TimeSeriesProfileInstanceDeleteController;
import cwms.cda.api.timeseriesprofile.TimeSeriesProfileParserCatalogController;
import cwms.cda.api.timeseriesprofile.TimeSeriesProfileParserController;
import cwms.cda.api.timeseriesprofile.TimeSeriesProfileParserCreateController;
import cwms.cda.api.timeseriesprofile.TimeSeriesProfileParserDeleteController;
import cwms.cda.api.watersupply.AccountingCatalogController;
import cwms.cda.api.watersupply.AccountingCreateController;
import cwms.cda.api.watersupply.WaterContractCatalogController;
import cwms.cda.api.watersupply.WaterContractController;
import cwms.cda.api.watersupply.WaterContractCreateController;
import cwms.cda.api.watersupply.WaterContractDeleteController;
import cwms.cda.api.watersupply.WaterContractTypeCatalogController;
import cwms.cda.api.watersupply.WaterContractTypeCreateController;
import cwms.cda.api.watersupply.WaterContractTypeDeleteController;
import cwms.cda.api.watersupply.WaterContractUpdateController;
import cwms.cda.api.watersupply.WaterPumpDisassociateController;
import cwms.cda.api.watersupply.WaterUserCatalogController;
import cwms.cda.api.watersupply.WaterUserController;
import cwms.cda.api.watersupply.WaterUserCreateController;
import cwms.cda.api.watersupply.WaterUserDeleteController;
import cwms.cda.api.watersupply.WaterUserUpdateController;
import cwms.cda.features.CdaFeatures;
import cwms.cda.formatters.Formats;
import cwms.cda.security.CdaAccessManager;
import cwms.cda.security.DataApiPrincipal;
import cwms.cda.security.MissingRolesException;
import cwms.cda.security.Role;
import io.javalin.Javalin;
import io.javalin.apibuilder.CrudFunction;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.apibuilder.CrudHandlerKt;
import io.javalin.core.security.RouteRole;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import org.jetbrains.annotations.NotNull;
import org.togglz.core.context.FeatureContext;

import javax.servlet.http.HttpServletResponse;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class ApiServletRouteConfiguration {

    private ApiServletRouteConfiguration() {
        throw new AssertionError("Utility class - do not instantiate");
    }

    public static void configureRoutes(MetricRegistry metrics, RouteRole[] requiredRoles, CdaAccessManager cdaAccessManager) {

        get("/", ctx -> ctx.result("Welcome to the CWMS REST API")
                .contentType(Formats.PLAIN));
        // Even view on this one requires authorization
        crud("/auth/keys/{key-name}",new ApiKeyController(metrics), new RouteRole[]{new Role(CAC_USER),
                new Role(CWMS_USERS_ROLE)});
        cdaCrudCache("/location/category/{category-id}",
                new LocationCategoryController(metrics), requiredRoles, 5, TimeUnit.MINUTES);
        cdaCrudCache("/location/group/{group-id}",
                new LocationGroupController(metrics), requiredRoles, 5, TimeUnit.MINUTES);
        get("/locations/with-kinds/", new LocationKindController(metrics));
        cdaCrudCache("/locations/{location-id}",
                new LocationController(metrics), requiredRoles, 5, TimeUnit.MINUTES);

        VerticalDatumController vdiController = new VerticalDatumController(metrics);
        String vdiPath = format("/location/{%s}/vertical-datum", Controllers.LOCATION_ID);
        get(vdiPath, ctx -> vdiController.getOne(ctx, ctx.pathParam(Controllers.LOCATION_ID)));
        addCacheControl(vdiPath, 5, TimeUnit.MINUTES);
        post(vdiPath, vdiController::create, requiredRoles);
        patch(vdiPath, ctx -> vdiController.update(ctx, ctx.pathParam(Controllers.LOCATION_ID)), requiredRoles);
        delete(vdiPath, ctx -> vdiController.delete(ctx, ctx.pathParam(Controllers.LOCATION_ID)), requiredRoles);
        cdaCrudCache("/entity/{entity-id}",
                new EntityController(metrics), requiredRoles, 5, TimeUnit.MINUTES);
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
        cdaCrudCache(format("/levels/{%s}", Controllers.LEVEL_ID),
                new LevelsController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        String levelTsPath = format("/levels/{%s}/timeseries", Controllers.LEVEL_ID);
        get(levelTsPath, new LevelsAsTimeSeriesController(metrics));
        addCacheControl(levelTsPath, 5, TimeUnit.MINUTES);
        String levelRefsPath = "/level-refs/";
        get(levelRefsPath, new LevelRefsController(metrics));
        addCacheControl(levelRefsPath, 5, TimeUnit.MINUTES);
        String recentPath = "/timeseries/recent/";
        get(recentPath, new TimeSeriesRecentController(metrics));
        addCacheControl(recentPath, 5, TimeUnit.MINUTES);

        String versionsPath = "/timeseries/versions/";
        get(versionsPath, new TimeSeriesVersionsController(metrics));
        addCacheControl(versionsPath, 5, TimeUnit.MINUTES);

        String filteredPath = "/timeseries/filtered";
        get(filteredPath, new TimeSeriesFilteredController(metrics));
        addCacheControl(filteredPath, 5, TimeUnit.MINUTES);

        cdaCrudCache(format("/standard-text-id/{%s}", Controllers.STANDARD_TEXT_ID),
                new StandardTextController(metrics), requiredRoles,1, TimeUnit.DAYS);

        String textTsPath = format("/timeseries/text/{%s}", NAME);
        cdaCrudCache(textTsPath, new TextTimeSeriesController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        String textValuePath = textTsPath + "/value";
        get(textValuePath, new TextTimeSeriesValueController(metrics));
        addCacheControl(textValuePath, 1, TimeUnit.DAYS);

        String binTsPath = format("/timeseries/binary/{%s}", NAME);
        cdaCrudCache(binTsPath, new BinaryTimeSeriesController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        String textBinaryValuePath = binTsPath + "/value";
        get(textBinaryValuePath, new BinaryTimeSeriesValueController(metrics));
        addCacheControl(textBinaryValuePath, 1, TimeUnit.DAYS);

        String timeSeriesProfilePath = "/timeseries/profile/";
        get(format("%s{%s}/{%s}", timeSeriesProfilePath, Controllers.LOCATION_ID, Controllers.PARAMETER_ID),
                new TimeSeriesProfileController(metrics));
        delete(format("%s/{%s}/{%s}", timeSeriesProfilePath, Controllers.LOCATION_ID,
                        Controllers.PARAMETER_ID), new TimeSeriesProfileDeleteController(metrics),
                requiredRoles);
        get(format(timeSeriesProfilePath, Controllers.LOCATION_ID, Controllers.PARAMETER_ID),
                new TimeSeriesProfileCatalogController(metrics));
        post(timeSeriesProfilePath, new TimeSeriesProfileCreateController(metrics), requiredRoles);

        String timeSeriesProfileParserPath = "/timeseries/profile-parser/";
        get(format("%s{%s}/{%s}/", timeSeriesProfileParserPath, Controllers.LOCATION_ID,
                Controllers.PARAMETER_ID), new TimeSeriesProfileParserController(metrics));
        post(timeSeriesProfileParserPath, new TimeSeriesProfileParserCreateController(metrics), requiredRoles);
        delete(format("%s{%s}/{%s}/", timeSeriesProfileParserPath, Controllers.LOCATION_ID,
                        Controllers.PARAMETER_ID), new TimeSeriesProfileParserDeleteController(metrics),
                requiredRoles);
        get(timeSeriesProfileParserPath, new TimeSeriesProfileParserCatalogController(metrics));

        String timeSeriesProfileInstancePath = "/timeseries/profile-instance/";
        get(format("%s{%s}/{%s}/{%s}/", timeSeriesProfileInstancePath, Controllers.LOCATION_ID,
                        Controllers.PARAMETER_ID, Controllers.VERSION),
                new TimeSeriesProfileInstanceController(metrics));
        post(timeSeriesProfileInstancePath, new TimeSeriesProfileInstanceCreateController(metrics), requiredRoles);
        delete(format("%s{%s}/{%s}/{%s}/", timeSeriesProfileInstancePath, Controllers.LOCATION_ID,
                        Controllers.PARAMETER_ID, Controllers.VERSION),
                new TimeSeriesProfileInstanceDeleteController(metrics), requiredRoles);
        get(timeSeriesProfileInstancePath, new TimeSeriesProfileInstanceCatalogController(metrics));

        cdaCrudCache("/timeseries/category/{category-id}",
                new TimeSeriesCategoryController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache(String.format("/timeseries/identifier-descriptor/{%s}", Controllers.TIMESERIES_ID),
                new TimeSeriesIdentifierDescriptorController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/timeseries/group/{group-id}",
                new TimeSeriesGroupController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/timeseries/{timeseries}",
                new TimeSeriesController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        addRatingHandlers(requiredRoles, metrics, cdaAccessManager);
        cdaCrudCache("/catalog/{dataset}",
                new CatalogController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/basins/{name}",
                new BasinController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache(format("/streams/{%s}", NAME),
                new StreamController(metrics), requiredRoles,5, TimeUnit.MINUTES);

        String downstreamLocations = format("/stream-locations/{%s}/{%s}/downstream-locations",
                Controllers.OFFICE, Controllers.NAME);
        get(downstreamLocations,new DownstreamLocationsGetController(metrics));
        addCacheControl(downstreamLocations, 5, TimeUnit.MINUTES);
        String upstreamLocations = format("/stream-locations/{%s}/{%s}/upstream-locations",
                Controllers.OFFICE, Controllers.NAME);

        get(upstreamLocations,new UpstreamLocationsGetController(metrics));
        addCacheControl(upstreamLocations, 5, TimeUnit.MINUTES);
        cdaCrudCache(format("/stream-locations/{%s}", NAME),
                new StreamLocationController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache(format("/stream-reaches/{%s}", NAME),
                new StreamReachController(metrics), requiredRoles,1, TimeUnit.DAYS);
        String measurements = "/measurements/";
        String measTimeExtents = measurements + "time-extents";
        get(measTimeExtents,new MeasurementTimeExtentsGetController(metrics));
        addCacheControl(measTimeExtents, 5, TimeUnit.MINUTES);
        cdaCrudCache(format("%s{%s}", measurements, LOCATION_ID),
                new cwms.cda.api.MeasurementController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache(format("/published/{%s}", LOCATION_ID),
                new PublishedController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/blobs/{blob-id}",
                new BlobController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/clobs/{clob-id}",
                new ClobController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/pools/{pool-id}",
                new PoolController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/specified-levels/{specified-level-id}",
                new SpecifiedLevelController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache(format("/forecast-instance/{%s}", Controllers.NAME),
                new ForecastInstanceController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        //-------Forecast Spec--------//
        String forecastSpecPath = "/forecast-spec/{%s}";
        cdaCrudCache(format(forecastSpecPath, Controllers.NAME),
                new ForecastSpecControllerV1(metrics), requiredRoles, 5, TimeUnit.MINUTES);
        cdaCrudCache(formatV2(forecastSpecPath, Controllers.NAME),
                new ForecastSpecControllerV2(metrics), requiredRoles, 5, TimeUnit.MINUTES);
        //----------------------------//
        String forecastFilePath = format("/forecast-instance/{%s}/file-data", NAME);
        get(forecastFilePath, new ForecastFileController(metrics));
        addCacheControl(forecastFilePath, 1, TimeUnit.DAYS);


        post(format("/projects/status-update/{%s}", NAME), new ProjectPublishStatusUpdate(metrics), requiredRoles);

        addWaterUserHandlers(format("/projects/{%s}/{%s}/water-user", OFFICE, PROJECT_ID), requiredRoles, metrics);
        addWaterContractHandlers(format("/projects/{%s}/{%s}/water-user/{%s}/contracts", OFFICE, PROJECT_ID,
                WATER_USER), requiredRoles, metrics);
        addAccountingHandlers(format("/projects/{%s}/{%s}/water-user/{%s}"
                + "/contracts/{%s}/accounting", OFFICE, PROJECT_ID, WATER_USER, CONTRACT_NAME), requiredRoles, metrics);
        delete(format("/projects/{%s}/{%s}/water-user/{%s}/contracts/{%s}/pumps/{%s}", OFFICE, PROJECT_ID,
                WATER_USER, CONTRACT_NAME, NAME), new WaterPumpDisassociateController(metrics), requiredRoles);
        addWaterContractTypeHandlers(format("/projects/{%s}/contract-types", OFFICE), requiredRoles, metrics);

        cdaCrudCache(format("/projects/embankments/{%s}", Controllers.NAME),
                new EmbankmentController(metrics), requiredRoles,1, TimeUnit.DAYS);
        cdaCrudCache(format("/projects/turbines/{%s}", Controllers.NAME),
                new TurbineController(metrics), requiredRoles,1, TimeUnit.DAYS);
        cdaCrudCache(format("/projects/locks/{%s}", Controllers.NAME),
                new LockController(metrics), requiredRoles,1, TimeUnit.DAYS);
        String turbineChanges = format("/projects/{%s}/{%s}/turbine-changes", Controllers.OFFICE, Controllers.NAME);
        get(turbineChanges,new TurbineChangesGetController(metrics));
        addCacheControl(turbineChanges, 5, TimeUnit.MINUTES);
        post(turbineChanges, new TurbineChangesPostController(metrics), requiredRoles);
        delete(turbineChanges, new TurbineChangesDeleteController(metrics), requiredRoles);

        String outletPath = format("/projects/outlets/{%s}", NAME);
        String gateChangePath = format("/projects/{%s}/{%s}/gate-changes", OFFICE,
                Controllers.PROJECT_ID);
        String gateChangeCreatePath = "/projects/gate-changes";

        cdaCrudCache(outletPath, new OutletController(metrics), requiredRoles, 1, TimeUnit.DAYS);
        post(gateChangeCreatePath, new GateChangeCreateController(metrics), requiredRoles);
        get(gateChangePath, new GateChangeGetAllController(metrics));
        delete(gateChangePath, new GateChangeDeleteController(metrics), requiredRoles);
        String virtualOutletPath = format("/projects/{%s}/{%s}/virtual-outlets/{%s}", OFFICE,
                Controllers.PROJECT_ID, NAME);
        cdaCrudCache(virtualOutletPath, new VirtualOutletController(metrics), requiredRoles, 1, TimeUnit.DAYS);
        String virtualOutletCreatePath = "/projects/virtual-outlets";
        post(virtualOutletCreatePath, new VirtualOutletCreateController(metrics), requiredRoles);

        get("/projects/locations/", new ProjectChildLocationHandler(metrics));
        cdaCrudCache(format("/projects/{%s}", Controllers.NAME),
                new ProjectController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache(format("/properties/{%s}", Controllers.NAME),
                new PropertyController(metrics), true, requiredRoles,1, TimeUnit.DAYS);
        cdaCrudCache(format("/lookup-types/{%s}", Controllers.NAME),
                new LookupTypeController(metrics), requiredRoles,1, TimeUnit.DAYS);

        addProjectLocksHandlers("/project-locks/{name}", requiredRoles, metrics);
        addProjectLockRightsHandlers("/project-lock-rights/{project-id}", requiredRoles, metrics);

        addUserManagementHandlers(metrics, cdaAccessManager);

        get("/version/", new CdaVersionHandler(metrics), requiredRoles);
        get(format("/rss/{%s}/{%s}", Controllers.OFFICE, Controllers.NAME), new RssHandler(metrics));
    }

    private static void addUserManagementHandlers(MetricRegistry metrics, CdaAccessManager cdaAccessManager) {
        RouteRole[] adminRoles = new RouteRole[] { new Role("CWMS User Admins")};
        RouteRole[] userRoles = new RouteRole[] {new Role(CWMS_USERS_ROLE), new Role(CAC_USER)};
        crud("/users/{user-name}", new UsersController(metrics), adminRoles);
        get("/roles", new GetRolesController(metrics), adminRoles);
        String userProfilePath = "/user/profile";
        get(userProfilePath, new UserProfileController(metrics), userRoles);
        cdaAccessManager.addCustomAuthorizer(userProfilePath, ApiServletRouteConfiguration::hasAnyRole);
        addUserListHandlers(userRoles, metrics, cdaAccessManager);
        post("/user/{user-name}/roles/{office-id}", new AddRoleController(metrics), adminRoles);
        delete("/user/{user-name}/roles/{office-id}", new DeleteRolesController(metrics), adminRoles);

    }

    private static void addUserListHandlers(RouteRole[] userRoles, MetricRegistry metrics, CdaAccessManager cdaAccessManager) {
        String userListCandidatesPath = "/user/list-member-candidates";
        String userListsPath = "/user/list";
        String userListPath = "/user/list/{user-list-id}";
        String userListMembersPath = "/user/list/{user-list-id}/members";
        String userListMemberPath = "/user/list/{user-list-id}/members/{user-id}";
        if (FeatureContext.getFeatureManager().isActive(CdaFeatures.USER_LISTS)) {
            get(userListCandidatesPath, new UserListCandidatesController(metrics), userRoles);
            get(userListsPath, new UserListsController(metrics), userRoles);
            post(userListsPath, new CreateUserListController(metrics), userRoles);
            get(userListPath, new UserListController(metrics), userRoles);
            patch(userListPath, new UpdateUserListController(metrics), userRoles);
            delete(userListPath, new DeleteUserListController(metrics), userRoles);
            get(userListMembersPath, new UserListMembersController(metrics), userRoles);
            post(userListMembersPath, new AddUserListMemberController(metrics), userRoles);
            delete(userListMemberPath, new UserListMemberController(metrics), userRoles);
        } else {
            get(userListCandidatesPath, ApiServletRouteConfiguration::userListsUnsupported, userRoles);
            get(userListsPath, ApiServletRouteConfiguration::userListsUnsupported, userRoles);
            post(userListsPath, ApiServletRouteConfiguration::userListsUnsupported, userRoles);
            get(userListPath, ApiServletRouteConfiguration::userListsUnsupported, userRoles);
            patch(userListPath, ApiServletRouteConfiguration::userListsUnsupported, userRoles);
            delete(userListPath, ApiServletRouteConfiguration::userListsUnsupported, userRoles);
            get(userListMembersPath, ApiServletRouteConfiguration::userListsUnsupported, userRoles);
            post(userListMembersPath, ApiServletRouteConfiguration::userListsUnsupported, userRoles);
            delete(userListMemberPath, ApiServletRouteConfiguration::userListsUnsupported, userRoles);
        }
        cdaAccessManager.addCustomAuthorizer(userListCandidatesPath, ApiServletRouteConfiguration::hasAnyRole);
        cdaAccessManager.addCustomAuthorizer(userListsPath, ApiServletRouteConfiguration::hasAnyRole);
        cdaAccessManager.addCustomAuthorizer(userListPath, ApiServletRouteConfiguration::hasAnyRole);
        cdaAccessManager.addCustomAuthorizer(userListMembersPath, ApiServletRouteConfiguration::hasAnyRole);
        cdaAccessManager.addCustomAuthorizer(userListMemberPath, ApiServletRouteConfiguration::hasAnyRole);
    }

    private static void userListsUnsupported(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED)
                .json(new CdaError("User lists are not enabled for this CDA deployment."));
    }

    private static Boolean hasAnyRole(DataApiPrincipal p, Set<RouteRole> roles) throws MissingRolesException {
        boolean retVal = roles.stream().anyMatch(p.getRoles()::contains);
        if (!retVal) {
            List<String> requiredRoleNames = roles.stream()
                    .map(Object::toString)
                    .collect(toList());
            throw new MissingRolesException(requiredRoleNames,
                    "Missing one of the following roles {" + String.join(",", requiredRoleNames) + "}");
        }
        return true;
    }

    /**
     * The POST handlers for /ratings/rate-* intentionally do not have
     * require roles. Instead they are rate limited if not authenticated.
     * POST is used as sending a body with GET is not standard and we cannot
     * be sure clients, or future servers, would correctly support that.
     * @param requiredRoles roles required for actions requiring authorization.
     */
    private static void addRatingHandlers(RouteRole[] requiredRoles, MetricRegistry metrics, CdaAccessManager cdaAccessManager) {

        String rateValues = format("/ratings/rate-values/{%s}/{%s}", OFFICE, RATING_ID);
        post(rateValues, new RateValuesController(metrics));
        String rateTs = format("/ratings/rate-ts/{%s}/{%s}", OFFICE, RATING_ID);
        post(rateTs, new RateTimeSeriesController(metrics));
        String reverseRateValues = format("/ratings/reverse-rate-values/{%s}/{%s}", OFFICE, RATING_ID);
        post(reverseRateValues, new ReverseRateValuesController(metrics));
        String reverseRateTs = format("/ratings/reverse-rate-ts/{%s}/{%s}", OFFICE, RATING_ID);
        post(reverseRateTs, new ReverseRateTimeSeriesController(metrics));
        cdaCrudCache("/ratings/template/{template-id}",
                new RatingTemplateController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/ratings/spec/{rating-id}",
                new RatingSpecController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        cdaCrudCache("/ratings/metadata/{rating-id}",
                new RatingMetadataController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        get("/ratings/{rating-id}/latest", new RatingLatestController(metrics));
        get("/ratings/effective-dates", new RatingEffectiveDatesController(metrics));
        cdaCrudCache("/ratings/{rating-id}",
                new RatingController(metrics), requiredRoles,5, TimeUnit.MINUTES);
        addRateLimit(rateTs, requiredRoles, cdaAccessManager);
        addRateLimit(reverseRateTs, requiredRoles, cdaAccessManager);
        addRateLimit(reverseRateValues, requiredRoles, cdaAccessManager);
        addRateLimit(rateValues, requiredRoles, cdaAccessManager);
    }

    /**
     * Add a rate limiter to a specified endpoint path, allowing authorized users to bypass the limit.
     *
     * @param path             the path to add the rate limiter to.
     * @param requiredRoles    the user roles required to access the path.
     * @param cdaAccessManager
     */
    private static void addRateLimit(String path, RouteRole[] requiredRoles, CdaAccessManager cdaAccessManager) {
        cdaAccessManager.addRateLimitedEndpoint(path, requiredRoles);
    }

    private static void addAccountingHandlers(String path, RouteRole[] requiredRoles, MetricRegistry metrics) {
        get(path, new AccountingCatalogController(metrics));
        post(path, new AccountingCreateController(metrics), requiredRoles);
    }

    private static void addProjectLocksHandlers(String path, RouteRole[] requiredRoles, MetricRegistry metrics) {
        String pathWithoutResource = path.replace(getResourceId(path), "");

        get(path, new ProjectLockGetOne(metrics), requiredRoles);
        get(pathWithoutResource, new ProjectLockCatalog(metrics), requiredRoles);
        post(pathWithoutResource + "deny", new ProjectLockRevokeDeny(metrics), requiredRoles);
        post(pathWithoutResource, new ProjectLockRequest(metrics), requiredRoles);
        post(pathWithoutResource + "release", new ProjectLockRelease(metrics), requiredRoles);
        delete(path, new ProjectLockRevoke(metrics), requiredRoles);
    }

    private static void addProjectLockRightsHandlers(String path, RouteRole[] requiredRoles, MetricRegistry metrics) {
        String pathWithoutResource = path.replace(getResourceId(path), "");
        get(pathWithoutResource, new LockRevokerRightsCatalog(metrics), requiredRoles);
        post(pathWithoutResource + "remove-all", new RemoveAllLockRevokerRights(metrics), requiredRoles);
        post(pathWithoutResource + "update", new UpdateLockRevokerRights(metrics), requiredRoles);

    }


    private static void addWaterUserHandlers(String path, RouteRole[] requiredRoles, MetricRegistry metrics) {
        get(path + format("/{%s}", WATER_USER), new WaterUserController(metrics), requiredRoles);
        get(path, new WaterUserCatalogController(metrics), requiredRoles);
        post(path, new WaterUserCreateController(metrics), requiredRoles);
        patch(path + format("/{%s}", WATER_USER), new WaterUserUpdateController(metrics), requiredRoles);
        delete(path + format("/{%s}", WATER_USER), new WaterUserDeleteController(metrics), requiredRoles);
    }

    private static void addWaterContractHandlers(String path, RouteRole[] requiredRoles, MetricRegistry metrics) {
        get(path + format("/{%s}", CONTRACT_NAME), new WaterContractController(metrics), requiredRoles);
        get(path, new WaterContractCatalogController(metrics), requiredRoles);
        post(path, new WaterContractCreateController(metrics), requiredRoles);
        patch(path + format("/{%s}", CONTRACT_NAME), new WaterContractUpdateController(metrics), requiredRoles);
        delete(path + format("/{%s}", CONTRACT_NAME), new WaterContractDeleteController(metrics), requiredRoles);
    }

    private static void addWaterContractTypeHandlers(String path, RouteRole[] requiredRoles, MetricRegistry metrics) {
        post(path, new WaterContractTypeCreateController(metrics), requiredRoles);
        get(path, new WaterContractTypeCatalogController(metrics), requiredRoles);
        delete(path + "/{display-value}", new WaterContractTypeDeleteController(metrics), requiredRoles);
    }

    /**
     * Given a path like "/location/category/{category-id}" this method returns "{category-id}".
     * @param fullPath the full path to extract the resource id from.
     * @return the resource id portion of the path.
     * @throws IllegalArgumentException if the path does not contain a resource id.
     */
    @NotNull
    private static String getResourceId(String fullPath) {
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
        // The segment immediately before the id is allowed to be a param itself (e.g. v2's
        // "{office}" segment on a primary resource) as long as there's a literal resource
        // base somewhere earlier in the path -- that's what actually anchors the route.
        boolean hasLiteralResourceBase = Arrays.stream(subPaths, 0, subPaths.length - 1)
                .anyMatch(segment -> !(segment.startsWith("{") || segment.startsWith("<")
                        || segment.endsWith("}") || segment.endsWith(">")));
        if (!hasLiteralResourceBase) {
            throw new IllegalArgumentException("CrudHandler requires a resource base at the "
                    + "beginning of the provided path, e.g. '/users/{user-id}' given: " + fullPath);
        }
        return resourceId;
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
    private static void cdaCrudCache(@NotNull String path, @NotNull CrudHandler crudHandler,
                                    @NotNull RouteRole[] roles, long duration, TimeUnit timeUnit) {
        cdaCrudCache(path, crudHandler, false, roles, duration, timeUnit);
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
     * @param getRequiresAuth if the get handlers should have an authorization check
     * @param roles the required these roles are present to access post, patch
     * @param duration the number of TimeUnit to cache GET responses.
     * @param timeUnit the TimeUnit to use for duration.
     */
    private static void cdaCrudCache(@NotNull String path, @NotNull CrudHandler crudHandler, boolean getRequiresAuth,
                                    @NotNull RouteRole[] roles, long duration, TimeUnit timeUnit) {
        cdaCrud(path, crudHandler, getRequiresAuth, roles);

        // path like /offices/{office} will match /offices/SWT getOne style url
        addCacheControl(path, duration, timeUnit);

        String pathWithoutResource = path.replace(getResourceId(path), "");
        // path like "/offices/" matches /offices getAll style url
        addCacheControl(pathWithoutResource, duration, timeUnit);
    }

    private static void addCacheControl(@NotNull String path, long duration, TimeUnit timeUnit) {
        if (timeUnit != null && duration > 0) {
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
    private static void cdaCrud(@NotNull String path, @NotNull CrudHandler crudHandler,
                               @NotNull RouteRole... roles) {
        cdaCrud(path, crudHandler, false, roles);
    }

    /**
     * This method is very similar to the ApiBuilder.crud method but the specified roles
     * are only required for the post, patch and delete methods.  getOne and getAll are always
     * allowed.
     * @param path where to register the routes.
     * @param crudHandler the handler requests should be forwarded to.
     * @param getRequiresAuth If all operations on this handler should have an authorization check
     * @param roles the accessmanager will require these roles are present to access post, patch
     *             and delete methods
     */
    private static void cdaCrud(@NotNull String path, @NotNull CrudHandler crudHandler,  boolean getRequiresAuth,
                               @NotNull RouteRole... roles) {
        String fullPath = prefixPath(path);
        String resourceId = getResourceId(fullPath);

        //noinspection KotlinInternalInJava
        Map<CrudFunction, Handler> crudFunctions = CrudHandlerKt.getCrudFunctions(crudHandler, resourceId);

        Javalin instance = staticInstance();
        // getOne and getAll are assumed not to need authorization
        String pathWithoutResource = fullPath.replace(resourceId, "");
        if (getRequiresAuth) {
            instance.get(fullPath, crudFunctions.get(CrudFunction.GET_ONE), roles);
            instance.get(pathWithoutResource, crudFunctions.get(CrudFunction.GET_ALL), roles);
        } else {
            instance.get(fullPath, crudFunctions.get(CrudFunction.GET_ONE));
            instance.get(pathWithoutResource, crudFunctions.get(CrudFunction.GET_ALL));
        }

        // create, update and delete need authorization.
        instance.post(pathWithoutResource, crudFunctions.get(CrudFunction.CREATE), roles);
        instance.patch(fullPath, crudFunctions.get(CrudFunction.UPDATE), roles);
        instance.delete(fullPath, crudFunctions.get(CrudFunction.DELETE), roles);
    }

    /**
     * Formats a v2 route path, per the standard that v2 primary-resource routes carry
     * {office} as a path segment immediately before the resource's own id segment, e.g.
     * {@code "/forecast-spec/{%s}"} becomes {@code "/v2/forecast-spec/{office}/{name}"}.
     * Sub-resources (nested under some other primary resource) do not get an office
     * segment of their own -- this only applies when formatting a primary resource's path.
     */
    private static String formatV2(String path, Object... args) {
        int lastSlash = path.lastIndexOf('/');
        String pathWithOffice = path.substring(0, lastSlash) + format("/{%s}", OFFICE) + path.substring(lastSlash);
        return format("/v2/" + pathWithOffice, args);
    }
}
