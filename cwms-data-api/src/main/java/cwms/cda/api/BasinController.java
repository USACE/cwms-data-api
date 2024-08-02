/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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

package cwms.cda.api;

import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.api.Controllers.requiredParamAs;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.basinconnectivity.BasinDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.basinconnectivity.Basin;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class BasinController implements CrudHandler {
    private static final Logger LOGGER = Logger.getLogger(BasinController.class.getName());
    public static final String TAG = "Basins";

    private final MetricRegistry metrics;

    public BasinController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the"
                    + " owning office of the basin whose data is to be included in the "
                    + "response. If this field is not specified, matching basin "
                    + "information from all offices shall be returned."),
            @OpenApiParam(name = UNIT, description = "Specifies the "
                    + "unit or unit system of the response. Valid values for the unit "
                    + "field are: "
                    + "\n* `EN`  Specifies English unit system. Basin "
                    + "values will be in the default English units for their parameters. "
                    + "(This is default if no value is entered)"
                    + "\n* `SI`  Specifies the"
                    + " SI unit system. Basin values will be in the default SI units for "
                    + "their parameters."),
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                content = {
                    @OpenApiContent(from = Basin.class, type = Formats.NAMED_PGJSON),
                    @OpenApiContent(from = cwms.cda.data.dto.basin.Basin.class, type = Formats.JSONV1),
                    @OpenApiContent(from = cwms.cda.data.dto.basin.Basin.class, type = Formats.JSON)
                }),
            @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                    + "parameters did not find a basin."),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                    + "implemented")
        },
        description = "Returns CWMS Basin Data",
        tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            String units =
                    ctx.queryParamAsClass(UNIT, String.class).getOrDefault(UnitSystem.EN.value());
            String office = ctx.queryParam(OFFICE);
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) :
                    Formats.JSONV1;

            ContentType contentType = Formats.parseHeader(formatHeader, Basin.class);
            String result;

            ctx.contentType(contentType.toString());
            if (contentType.getType().equals(Formats.NAMED_PGJSON)) {
                BasinDao basinDao = new BasinDao(dsl);
                List<Basin> basins = basinDao.getAllBasins(units, office);
                result = Formats.format(contentType, basins, Basin.class);
                ctx.result(result);
                ctx.status(HttpServletResponse.SC_OK);
            } else {
                cwms.cda.data.dao.basin.BasinDao basinDao = new cwms.cda.data.dao.basin.BasinDao(dsl);
                List<cwms.cda.data.dto.basin.Basin> basins = basinDao.getAllBasins(units, office);
                result = Formats.format(contentType, basins, cwms.cda.data.dto.basin.Basin.class);
                ctx.result(result);
                ctx.status(HttpServletResponse.SC_OK);
            }
        } catch (SQLException ex) {
            CdaError error = new CdaError("Error retrieving all basins");
            LOGGER.log(Level.SEVERE, "Error retrieving all basins", ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(error);
        }
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the"
                    + " owning office of the basin whose data is to be included in the "
                    + "response. If this field is not specified, matching basin "
                    + "information from all offices shall be returned."),
            @OpenApiParam(name = UNIT, description = "Specifies the "
                    + "unit or unit system of the response. Valid values for the unit "
                    + "field are:"
                    + "\n* `EN`  Specifies English unit system. Basin values will be in "
                    + "the default English units for their parameters. "
                    + "(This is default if no value is entered)"
                    + "\n* `SI`  Specifies the SI unit system. Basin values will be in "
                    + "the default SI units for "
                    + "their parameters."),
        },
        pathParams = {
            @OpenApiParam(name = NAME, description = "Specifies the name of "
                    + "the basin to be retrieved.")
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                content = {
                    @OpenApiContent(from = Basin.class, type = Formats.NAMED_PGJSON),
                    @OpenApiContent(from = cwms.cda.data.dto.basin.Basin.class, type = Formats.JSONV1),
                    @OpenApiContent(from = cwms.cda.data.dto.basin.Basin.class, type = Formats.JSON)
                }),
            @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                    + "parameters did not find a basin."),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                    + "implemented")
        },
        description = "Returns CWMS Basin Data",
        tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String name) {

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);

            String units =
                    ctx.queryParamAsClass(UNIT, String.class).getOrDefault(UnitSystem.EN.value());
            String office = ctx.queryParam(OFFICE);
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) :
                    Formats.NAMED_PGJSON;
            ContentType contentType = Formats.parseHeader(formatHeader, Basin.class);
            ctx.contentType(contentType.toString());
            String result;

            if (contentType.getType().equals(Formats.NAMED_PGJSON)) {
                BasinDao basinDao = new BasinDao(dsl);
                Basin basin = basinDao.getBasin(name, units, office);
                result = Formats.format(contentType, Collections.singletonList(basin), Basin.class);
                ctx.result(result);
                ctx.status(HttpServletResponse.SC_OK);
            } else {
                cwms.cda.data.dao.basin.BasinDao basinDao = new cwms.cda.data.dao.basin.BasinDao(dsl);
                CwmsId basinId = new CwmsId.Builder()
                        .withName(name)
                        .withOfficeId(office)
                        .build();
                cwms.cda.data.dto.basin.Basin basin = basinDao.getBasin(basinId, units);
                result = Formats.format(contentType, basin);
                ctx.result(result);
                ctx.status(HttpServletResponse.SC_OK);
            }
        } catch (SQLException ex) {
            CdaError error = new CdaError("Error retrieving " + name);
            String errorMsg = "Error retrieving " + name;
            LOGGER.log(Level.SEVERE, errorMsg, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(error);
        }
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = NAME, required = true, description = "Specifies the new name for the basin.")
        },
        pathParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                + "the basin to be renamed."),
            @OpenApiParam(name = NAME, description = "Specifies the name of "
                    + "the basin to be renamed.")
        },
        responses = {
            @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                    + "parameters did not find a basin."),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                    + "implemented")
        },
        method = HttpMethod.PATCH,
        description = "Renames CWMS Basin",
        tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String name) {
        DSLContext dsl = getDslContext(ctx);
        String officeId = requiredParam(ctx, OFFICE);
        String newStreamId = requiredParam(ctx, NAME);
        cwms.cda.data.dao.basin.BasinDao basinDao = new cwms.cda.data.dao.basin.BasinDao(dsl);
        CwmsId oldLoc = new CwmsId.Builder()
            .withName(name)
            .withOfficeId(officeId)
            .build();
        CwmsId newLoc = new CwmsId.Builder()
            .withName(newStreamId)
            .withOfficeId(officeId)
            .build();
        basinDao.renameBasin(oldLoc, newLoc);
        ctx.status(HttpServletResponse.SC_OK).json("Updated Location");
    }

    @OpenApi(
        responses = {
            @OpenApiResponse(status = STATUS_204, description = "Basin successfully stored to CWMS."),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not implemented")
        },
        method = HttpMethod.POST,
        description = "Creates CWMS Basin",
        tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        DSLContext dsl = getDslContext(ctx);
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) :
                Formats.JSONV1;
        ContentType contentType = Formats.parseHeader(formatHeader, cwms.cda.data.dto.basin.Basin.class);
        ctx.contentType(contentType.toString());

        cwms.cda.data.dto.basin.Basin basin = Formats.parseContent(contentType, ctx.body(),
                cwms.cda.data.dto.basin.Basin.class);

        String newBasinId = basin.getBasinId().getName();
        cwms.cda.data.dao.basin.BasinDao basinDao = new cwms.cda.data.dao.basin.BasinDao(dsl);
        basinDao.storeBasin(basin);
        ctx.status(HttpServletResponse.SC_CREATED).json(newBasinId + " Created");
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the"
                    + " owning office of the basin to be renamed."),
            @OpenApiParam(name = METHOD, required = true, description = "Specifies the delete method used.",
                type = JooqDao.DeleteMethod.class)
        },
        pathParams = {
            @OpenApiParam(name = NAME, description = "Specifies the name of "
                    + "the basin to be deleted.")
        },
        responses = {
            @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                    + "parameters did not find a basin."),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                    + "implemented")
        },
        description = "Renames CWMS Basin",
        tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String name) {
        DSLContext dsl = getDslContext(ctx);
        JooqDao.DeleteMethod deleteMethod = requiredParamAs(ctx, METHOD, JooqDao.DeleteMethod.class);
        cwms.cda.data.dao.basin.BasinDao basinDao = new cwms.cda.data.dao.basin.BasinDao(dsl);
        CwmsId basinId = new CwmsId.Builder()
                .withName(name)
                .withOfficeId(ctx.queryParam(OFFICE))
                .build();
        cwms.cda.data.dto.basin.Basin retBasin = basinDao.getBasin(basinId, "EN");
        if (retBasin == null) {
            CdaError error = new CdaError("No matching basin " + name);
            ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
            return;
        }
        basinDao.deleteBasin(basinId, deleteMethod.getRule().getRule());
        ctx.status(HttpServletResponse.SC_NO_CONTENT).json(basinId.getName() + " Deleted");
    }
}