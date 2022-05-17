package cwms.radar.api;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.SpecifiedLevelDao;
import cwms.radar.data.dto.SpecifiedLevel;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.JooqDao.getDslContext;


public class SpecifiedLevelController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(SpecifiedLevelController.class.getName());
    private final MetricRegistry metrics;


    private final Histogram requestResultSize;

    public SpecifiedLevelController(MetricRegistry metrics){
        this.metrics=metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className,"results","size")));
    }

    @NotNull
    protected SpecifiedLevelDao getDao(DSLContext dsl)
    {
        return new SpecifiedLevelDao(dsl);
    }


    private Timer.Context markAndTime(String subject)
    {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }
    
    
    @OpenApi(
        queryParams = {
            @OpenApiParam(name="office", required=false, description="Specifies the owning office of the Specified Levels whose data is to be included in the response. If this field is not specified, matching rating information from all offices shall be returned."),
            @OpenApiParam(name="template-id-mask", required=false, description="Mask that specifies the IDs to be included in the response. If this field is not specified, all specified levels shall be returned."),
        },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(type = Formats.JSON, from = SpecifiedLevel.class)
                            }

                    )},
        tags = {"Specified Levels"}
    )
    @Override
    public void getAll(Context ctx)
    {
        String office = ctx.queryParam("office");
        String templateIdMask = ctx.queryParam("template-id-mask");
        
        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");
        try(final Timer.Context timeContext = markAndTime("getAll"); DSLContext dsl = getDslContext(ctx))
        {
            SpecifiedLevelDao dao = getDao(dsl);
            List<SpecifiedLevel> levels = dao.getSpecifiedLevels(office, templateIdMask);

            ctx.contentType(contentType.toString());

            String result = Formats.format(contentType, levels, SpecifiedLevel.class);
            ctx.result(result);
            requestResultSize.update(result.length());
            ctx.status(HttpServletResponse.SC_OK);
        }
        catch(Exception ex)
        {
            RadarError re = new RadarError("Failed to process request: " + ex.getLocalizedMessage());
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }


    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String templateId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Specs.
    }
    

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx)
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Specs.
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String locationCode)
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Specs.
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String locationCode)
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Specs.
    }
    
}
