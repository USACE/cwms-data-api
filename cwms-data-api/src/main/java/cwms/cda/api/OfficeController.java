package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.ACCEPT;
import static cwms.cda.api.Controllers.FORMAT;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.HAS_DATA;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.OfficeDao;
import cwms.cda.data.dto.Office;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.OfficeFormatV1;
import cwms.cda.formatters.xml.XMLv1Office;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;

/**
 * Handles all Office CRUD methods.
 * 
 * @see OfficeController
 */
public class OfficeController implements CrudHandler {

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    /**
     * Sets up Office endpoint metrics for the controller.
     * 
     * @param metrics set the MetricRegistry for this class
     */
    public OfficeController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = OfficeController.class.getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(queryParams = {
        @OpenApiParam(name = FORMAT,
            deprecated = true,
            description = "(CWMS-Data-Format-Deprecated: 2024-11-01 in favor of Accept header) Specifies the encoding "
                + "format of the response. Valid value for the format field for this "
                + "URI are:\r\n"
                    + "\n* `tab`\r\n"
                    + "\n* `csv`\r\n "
                    + "\n* `xml`\r\n"
                    + "\n* `json` (default)"),
        @OpenApiParam(name = HAS_DATA,
            description = "A flag ('True'/'False') "
                + "When set to true this returns offices that have operational data. "
                + "Default value is <b>False</b>,. "
                + "<a href=\"https://github.com/USACE/cwms-data-api/issues/321\" "
                + "target=\"_blank\">Feature #321</a>",
            type = Boolean.class)
        }, responses = {
            @OpenApiResponse(status = STATUS_200,
                description = "A list of offices.",
                content = {
                    @OpenApiContent(from = OfficeFormatV1.class, type = ""),
                    @OpenApiContent(from = Office.class, isArray = true, type = Formats.JSON),
                    @OpenApiContent(from = OfficeFormatV1.class, isArray = true, type = Formats.JSONV1),
                    @OpenApiContent(from = Office.class, isArray = true, type = Formats.JSONV2),
                }),
        }, tags = { "Offices" }
    )
    @Override
    public void getAll(Context ctx) {

        try (final Timer.Context timeContext = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);

            OfficeDao dao = new OfficeDao(dsl);
            String formatParm = ctx
                .queryParamAsClass(FORMAT, String.class)
                .getOrDefault("");
            Boolean hasDataParm = ctx
                .queryParamAsClass(HAS_DATA, Boolean.class)
                .getOrDefault(false);
            List<Office> offices = dao.getOffices(hasDataParm);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm, Office.class);

            String result = Formats.format(contentType, offices, Office.class);

            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());

        }
    }

    @OpenApi(
            pathParams = @OpenApiParam(name = OFFICE, description = "The 3 letter office ID you"
                    + " want more information for"),
            queryParams = @OpenApiParam(name = FORMAT,
                    deprecated = true,
                    description = "(CWMS-Data-Format-Deprecated: 2024-11-01 in favor of Accept header)"
                            + " Specifies the encoding format of the response. Valid value for the format "
                            + "field for this URI are:\r\n"
                                + "1. `tab`\r\n"
                                + "2. `csv`\r\n"
                                + "3. `xml`\r\n"
                                + "4. `json` (default)"
            ),
            responses = {@OpenApiResponse(status = STATUS_200,
                    description = "A list of offices.",
                    content = {
                        @OpenApiContent(from = OfficeFormatV1.class, type = ""),
                        @OpenApiContent(from = Office.class, type = Formats.JSON),
                        @OpenApiContent(from = OfficeFormatV1.class, type = Formats.JSONV1),
                        @OpenApiContent(from = Office.class, type = Formats.JSONV2),
                        @OpenApiContent(from = Office.class, type = Formats.XML),
                        @OpenApiContent(from = OfficeFormatV1.class, type = Formats.XMLV1),
                        @OpenApiContent(from = Office.class, type = Formats.XMLV2)
                    })
            }, tags = { "Offices" })
    @Override
    public void getOne(Context ctx, String officeId) {
        try (final Timer.Context timeContext = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);

            OfficeDao dao = new OfficeDao(dsl);
            Optional<Office> office = dao.getOfficeById(officeId);
            if (office.isPresent()) {
                String formatParm = ctx.queryParamAsClass(FORMAT, String.class).getOrDefault("");
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm, Office.class);
                String result = Formats.format(contentType, office.get());
                ctx.result(result).contentType(contentType.toString());

                requestResultSize.update(result.length());
            } else {
                Map<String, String> map = new HashMap<>();
                map.put(OFFICE, "An office with that name does not exist");
                CdaError re = new CdaError("Not Found", map);
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String officeId) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String officeId) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

}
