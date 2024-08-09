package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.ID_MASK;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.queryParamAsClass;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.project.Projects;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class ProjectController implements CrudHandler {
    public static final Logger logger = Logger.getLogger(ProjectController.class.getName());
    private static final int DEFAULT_PAGE_SIZE = 100;
    public static final String TAG = "Projects";

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public ProjectController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(queryParams = {
        @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the data"
                + " in the response. If this field is not specified, matching items from all"
                + " offices shall be returned."),
        @OpenApiParam(name = ID_MASK, description = "Project Id mask."),
        @OpenApiParam(name = PAGE,
                description = "This end point can return a lot of data, this identifies where"
                        + " in the request you are. This is an opaque value, and can be"
                        + " obtained from the 'next-page' value in the response."),
        @OpenApiParam(name = PAGE_SIZE, type = Integer.class,
                description = "How many entries per page returned. "
                        + "Default " + DEFAULT_PAGE_SIZE + ".")
        },
        responses = {
            @OpenApiResponse(status = STATUS_200, content = {
                @OpenApiContent(type = Formats.JSON, from = Projects.class)}),
            @OpenApiResponse(status = STATUS_404, description = "Based on the combination of"
                    + " inputs provided the projects were not found."),
            @OpenApiResponse(status = STATUS_501, description = "request format is not"
                    + " implemented")},
        description = "Returns Projects Data",
        tags = {TAG})
    @Override
    public void getAll(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);

            ProjectDao dao = new ProjectDao(dsl);
            String office = ctx.queryParam(OFFICE);

            String projectIdMask = ctx.queryParam(ID_MASK);

            String cursor = queryParamAsClass(ctx, new String[]{PAGE},
                    String.class, "", metrics, name(ProjectController.class.getName(),
                            GET_ALL));

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE},
                    Integer.class, DEFAULT_PAGE_SIZE, metrics,
                    name(ProjectController.class.getName(), GET_ALL));

            Projects projects = dao.retrieveProjectsFromTable(cursor, pageSize, projectIdMask, office);

            ContentType contentType = getContentType(ctx);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, projects);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());

        }

    }

    private static @NotNull ContentType getContentType(Context ctx) {
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSON;
        return Formats.parseHeader(formatHeader, Project.class);
    }

    @OpenApi(
            pathParams = {
                @OpenApiParam(name = NAME, required = true, description = "Specifies the"
                        + " project to be included in the response."),
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the"
                        + " owning office of the Project whose data is to be included in the"
                        + " response."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(from = Project.class, type = Formats.JSONV1)
                }),
                @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                        + "inputs provided the Project was not found."),
                @OpenApiResponse(status = STATUS_501, description = "request format is not "
                        + "implemented")},
            description = "Retrieves requested Project", tags = {"Projects"})
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String name) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);

            ProjectDao dao = new ProjectDao(dsl);

            // These are required
            String office = requiredParam(ctx, OFFICE);

            Project project = dao.retrieveProject(office, name);

            if (project == null) {
                CdaError re = new CdaError("Unable to find Project based on parameters given");
                logger.info(() -> {
                    String fullUrl = ctx.fullUrl();
                    return re + System.lineSeparator() + "for request " + fullUrl;
                });
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            } else {
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeader(formatHeader, Project.class);
                ctx.contentType(contentType.toString());

                String result = Formats.format(contentType, project);

                ctx.result(result);
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            }
        }
    }

    @OpenApi(
            description = "Create new Project",
            requestBody = @OpenApiRequestBody(required = true,
                content = {
                    @OpenApiContent(from = Project.class, type = Formats.JSONV1),
                    @OpenApiContent(from = Project.class, type = Formats.JSON)
                }
            ),
            queryParams = {
                @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                    description = "Create will fail if provided ID already exists. Default: true")
            },
            method = HttpMethod.POST,
            tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);

            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSON;
            ContentType contentType = Formats.parseHeader(formatHeader, Project.class);
            Project project = Formats.parseContent(contentType, ctx.body(), Project.class);
            ProjectDao dao = new ProjectDao(dsl);

            dao.create(project, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED);
        }
    }

    @OpenApi(
            description = "Updates a project",
            pathParams = {
                @OpenApiParam(name = NAME, description = "The id of the project to be updated"),
            },
            requestBody = @OpenApiRequestBody(
                content = {
                    @OpenApiContent(from = Project.class, type = Formats.JSON),
                    @OpenApiContent(from = Project.class, type = Formats.JSONV1),
                },
                required = true
            ),
            method = HttpMethod.PATCH,
            tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String name) {

        try (Timer.Context ignored = markAndTime(UPDATE)) {
            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSON;
            ContentType contentType = Formats.parseHeader(formatHeader, Project.class);
            Project project = Formats.parseContent(contentType, ctx.body(), Project.class);
            DSLContext dsl = getDslContext(ctx);

            ProjectDao dao = new ProjectDao(dsl);
            dao.update(project);
        }
    }


    @OpenApi(
            description = "Deletes requested reservoir project",
            pathParams = {
                @OpenApiParam(name = NAME, description = "The project identifier to be deleted"),
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                        + "owning office of the project to be deleted"),
                @OpenApiParam(name = METHOD, type = JooqDao.DeleteMethod.class,
                        description = "Specifies the delete method used. "
                                + "Defaults to \"DELETE_KEY\"")
            },
            method = HttpMethod.DELETE,
            tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String name) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String office = requiredParam(ctx, OFFICE);

            JooqDao.DeleteMethod deleteMethod = ctx.queryParamAsClass(METHOD, JooqDao.DeleteMethod.class)
                    .getOrDefault(JooqDao.DeleteMethod.DELETE_KEY);

            ProjectDao dao = new ProjectDao(dsl);
            dao.delete(office, name, deleteMethod.getRule());

            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }


}
