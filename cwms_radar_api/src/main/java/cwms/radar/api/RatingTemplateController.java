package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;

import static cwms.radar.api.Controllers.DELETE;
import static cwms.radar.api.Controllers.GET_ALL;
import static cwms.radar.api.Controllers.GET_ONE;
import static cwms.radar.api.Controllers.METHOD;
import static cwms.radar.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.radar.api.Controllers.OFFICE;
import static cwms.radar.api.Controllers.PAGE;
import static cwms.radar.api.Controllers.PAGE_SIZE;
import static cwms.radar.api.Controllers.RESULTS;
import static cwms.radar.api.Controllers.SIZE;
import static cwms.radar.api.Controllers.TEMPLATE_ID;
import static cwms.radar.api.Controllers.TEMPLATE_ID_MASK;
import static cwms.radar.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.RatingTemplateDao;
import cwms.radar.data.dto.rating.RatingSpec;
import cwms.radar.data.dto.rating.RatingTemplate;
import cwms.radar.data.dto.rating.RatingTemplates;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class RatingTemplateController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(RatingTemplateController.class.getName());

    private final MetricRegistry metrics;

    private static final int DEFAULT_PAGE_SIZE = 100;

    private final Histogram requestResultSize;

    public RatingTemplateController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }


    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }


    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, description = "Specifies the owning office of "
                            + "the Rating Templates whose data is to be included in the response."
                            + " If this field is not specified, matching rating information from "
                            + "all offices shall be returned."),
                    @OpenApiParam(name = TEMPLATE_ID_MASK, description = "RegExp that specifies"
                            + " the rating template IDs to be included in the response. If this "
                            + "field is not specified, all rating templates shall be returned."),
                    @OpenApiParam(name = PAGE,
                            description = "This end point can return a lot of data, this "
                                    + "identifies where in the request you are. This is an opaque"
                                    + " value, and can be obtained from the 'next-page' value in "
                                    + "the response."
                    ),
                    @OpenApiParam(name = PAGE_SIZE,
                            type = Integer.class,
                            description = "How many entries per page returned. "
                                    + "Default " + DEFAULT_PAGE_SIZE + "."
                    ),
            },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(from = RatingTemplates.class, type =
                                            Formats.JSONV2),
                            }
                    )},
            tags = {"Ratings"}
    )
    @Override
    public void getAll(Context ctx) {
        String cursor = ctx.queryParamAsClass(PAGE, String.class).getOrDefault("");
        int pageSize =
                ctx.queryParamAsClass(PAGE_SIZE, Integer.class).getOrDefault(DEFAULT_PAGE_SIZE);

        String office = ctx.queryParam(OFFICE);
        String templateIdMask = ctx.queryParam(TEMPLATE_ID_MASK);

        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");
        try (final Timer.Context timeContext = markAndTime(GET_ALL);
             DSLContext dsl = getDslContext(ctx)) {
            RatingTemplateDao ratingTemplateDao = getRatingTemplateDao(dsl);
            RatingTemplates ratingTemplates = ratingTemplateDao.retrieveRatingTemplates(cursor,
                    pageSize, office,
                    templateIdMask);
            ctx.status(HttpServletResponse.SC_OK);
            ctx.contentType(contentType.toString());

            String result = Formats.format(contentType, ratingTemplates);
            ctx.result(result);
            requestResultSize.update(result.length());
        } catch (Exception ex) {
            RadarError re =
                    new RadarError("Failed to process request: " + ex.getLocalizedMessage());
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

    }

    @NotNull
    private RatingTemplateDao getRatingTemplateDao(DSLContext dsl) {
        return new RatingTemplateDao(dsl);
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = TEMPLATE_ID, required = true, description = "Specifies"
                            + " the template whose data is to be included in the response")
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the Rating Templates whose data is to be included"
                            + " in the response. If this field is not specified, matching rating "
                            + "information from all offices shall be returned."),
            },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(isArray = true, from = RatingTemplate.class,
                                            type = Formats.JSONV2),
                            }
                    )},
            tags = {"Ratings"}
    )
    @Override
    public void getOne(Context ctx, String templateId) {
        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");

        String office = ctx.queryParam(OFFICE);

        try (final Timer.Context timeContext = markAndTime(GET_ONE);
             DSLContext dsl = getDslContext(ctx)) {
            RatingTemplateDao ratingSetDao = getRatingTemplateDao(dsl);

            Optional<RatingTemplate> template = ratingSetDao.retrieveRatingTemplate(office,
                    templateId);
            if (template.isPresent()) {
                String result = Formats.format(contentType, template.get());

                ctx.result(result);
                ctx.contentType(contentType.toString());

                requestResultSize.update(result.length());
                ctx.status(HttpServletResponse.SC_OK);
            } else {
                RadarError re = new RadarError("Unable to find Rating Template based on "
                        + "parameters given");
                logger.info(() -> re + System.lineSeparator() + "for request " + ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }
        }
    }


    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of
        // generated methods, choose Tools | Templates.
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String locationCode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of
        // generated methods, choose Tools | Templates.
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String locationCode) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of
        // generated methods, choose Tools | Templates.
    }

}
