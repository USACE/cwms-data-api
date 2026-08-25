package cwms.cda.api.forecast;

import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.DESIGNATOR;
import static cwms.cda.api.Controllers.DESIGNATOR_MASK;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.ID_MASK;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.SOURCE_ENTITY;
import static cwms.cda.api.Controllers.SOURCE_ENTITY_LIKE;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.BaseCrudHandler;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.ExceptionTraceSupport;
import cwms.cda.data.dao.forecast.ForecastSpecDao;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import java.io.IOException;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public abstract class ForecastSpecController<T extends CwmsDTOBase> extends BaseCrudHandler {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();

    static final String TAG = "Forecast";

    protected ForecastSpecController(MetricRegistry metrics) {
        super(metrics);
    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    /** Builds the version-specific DAO for this request. */
    protected abstract ForecastSpecDao<T> newDao(DSLContext dsl);

    /** The DTO type this controller reads and writes */
    protected abstract Class<T> getDtoClass();

    /**
     * Where to read the required office from for delete/getOne. Defaults to the
     * {@code office} query param. V2 (and future versions that put office in the path per
     * the v2 primary-resource standard) should override this to read {@code ctx.pathParam}
     * instead.
     */
    protected String requireOffice(Context ctx) {
        return requiredParam(ctx, OFFICE);
    }

    /**
     * Where to read the optional office filter from for getAll. Defaults to the
     * {@code office} query param (nullable, meaning "any office"). V2 overrides this since
     * office is a required path segment there, not an optional filter.
     */
    protected String optionalOffice(Context ctx) {
        return ctx.queryParam(OFFICE);
    }

    @Override
    public void create(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao<T> dao = newDao(dsl);
            T forecastSpec = deserializeForecastSpec(ctx);

            dao.create(forecastSpec);

            ctx.status(HttpServletResponse.SC_CREATED);
        }
    }

    @Override
    public void delete(@NotNull Context ctx, @NotNull String name) {
        String office = requireOffice(ctx);
        String designator = ctx.queryParamAsClass(DESIGNATOR, String.class).allowNullable().get();

        JooqDao.DeleteMethod deleteMethod = ctx.queryParamAsClass(METHOD, JooqDao.DeleteMethod.class)
                .getOrDefault(JooqDao.DeleteMethod.DELETE_KEY);
        DeleteRule deleteRule;
        switch (deleteMethod) {
            case DELETE_ALL:
                deleteRule = DeleteRule.DELETE_ALL;
                break;
            case DELETE_DATA:
                deleteRule = DeleteRule.DELETE_DATA;
                break;
            case DELETE_KEY:
                deleteRule = DeleteRule.DELETE_KEY;
                break;
            default:
                throw new IllegalArgumentException("Delete Method provided does not match accepted rule constants: "
                        + deleteMethod);
        }
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao<T> dao = newDao(dsl);

            dao.delete(office, name, designator, deleteRule);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

    @Override
    public void getAll(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            String office = optionalOffice(ctx);
            String names = ctx.queryParamAsClass(ID_MASK, String.class).getOrDefault("*");
            String designator = ctx.queryParamAsClass(DESIGNATOR_MASK, String.class).allowNullable().get();
            String sourceEntity = ctx.queryParamAsClass(SOURCE_ENTITY, String.class).getOrDefault("*");
            String entityLike = ctx.queryParamAsClass(SOURCE_ENTITY_LIKE, String.class).allowNullable().get();

            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao<T> dao = newDao(dsl);

            List<T> specs = dao.getForecastSpecs(office, names, designator, sourceEntity, entityLike);

            writeResponse(ctx, specs);
        } catch (IOException ex) {
            handleWriteFailure(ctx, ex, "Failed to process request to retrieve forecast specs");
        }
    }

    @Override
    public void getOne(@NotNull Context ctx, @NotNull String name) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            String office = requireOffice(ctx);
            String designator = ctx.queryParamAsClass(DESIGNATOR, String.class).allowNullable().get();

            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao<T> dao = newDao(dsl);

            T spec = dao.getForecastSpec(office, name, designator);

            writeResponse(ctx, spec);
        } catch (IOException ex) {
            handleWriteFailure(ctx, ex, "Failed to process request to retrieve forecast spec");
        }
    }

    @Override
    public void update(@NotNull Context ctx, @NotNull String name) {
        logUnusedPathParameter(ctx, NAME, "Body contains information");
        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            T forecastSpec = deserializeForecastSpec(ctx);
            DSLContext dsl = getDslContext(ctx);
            ForecastSpecDao<T> dao = newDao(dsl);
            dao.update(forecastSpec);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    private void writeResponse(Context ctx, List<T> specs) throws IOException {
        ContentType contentType = Formats.parseHeader(ctx.header(Header.ACCEPT), getDtoClass());
        writeBytes(ctx, contentType, Formats.format(contentType, specs, getDtoClass()));
    }

    private void writeResponse(Context ctx, T spec) throws IOException {
        ContentType contentType = Formats.parseHeader(ctx.header(Header.ACCEPT), getDtoClass());
        writeBytes(ctx, contentType, Formats.format(contentType, spec));
    }

    private void writeBytes(Context ctx, ContentType contentType, String result) throws IOException {
        updateResultSize(result.length());

        ctx.status(HttpServletResponse.SC_OK);
        ctx.contentType(contentType.toString());

        byte[] bytes = result.getBytes();
        ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
        ctx.res.getOutputStream().write(bytes);
    }

    private void handleWriteFailure(Context ctx, IOException ex, String message) {
        CdaError error = ExceptionTraceSupport.buildError(ctx, message, ex);
        LOGGER.atSevere().withCause(ex).log("%s (handler: %s)", message, getClass().getSimpleName());
        ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(error);
    }

    private T deserializeForecastSpec(Context ctx) {
        ContentType contentType = Formats.parseHeader(ctx.req.getContentType(), getDtoClass());
        return Formats.parseContent(contentType, ctx.body(), getDtoClass());
    }
}
