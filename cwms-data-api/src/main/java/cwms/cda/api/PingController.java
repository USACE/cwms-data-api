package cwms.cda.api;

import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;

import cwms.cda.data.dto.Clob;
import cwms.cda.data.dto.Pools;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;

public class PingController implements CrudHandler {

    private static final Map<String, String> entries = new LinkedHashMap<>();

    public static final String TAG = "Ping";

    private static final AtomicInteger next = new AtomicInteger(0);


    @OpenApi(
            description = "Create new Ping",
            requestBody = @OpenApiRequestBody(
                    content = {@OpenApiContent(type = Formats.JSON)},
                    required = false),
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(type = Formats.JSON)}),
                    },
            method = HttpMethod.POST,
            tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        int nextId = next.incrementAndGet();

        String id = Integer.toString(nextId);
        entries.put(id, getNowStr());
        ctx.json(entries.get(id));
        ctx.status(200);
    }

    @OpenApi(
            description = "Deletes a ping",
            pathParams = {
                    @OpenApiParam(name = "id", description = "The id to be deleted"),
            },
            responses = {
                    @OpenApiResponse(status = "204", content = { @OpenApiContent(type = Formats.JSON)}),
                    @OpenApiResponse(status = STATUS_404),
            },
            method = HttpMethod.DELETE,
            tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String id) {

        if (entries.containsKey(id)) {
            entries.remove(id);
            ctx.status(204);
        } else {
            ctx.status(404);
        }
    }

    @OpenApi(
            tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context context) {
        context.json(entries);
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = "id", required = true, description = "Specifies the id."),
            },
            responses = {
                    @OpenApiResponse(status = "200", content = { @OpenApiContent(type = Formats.JSON)}),
                    @OpenApiResponse(status = STATUS_404),
            },
            description = "Retrieves a single ping entry or 404 if not found.", tags = {"Pools"})
    @Override
    public void getOne(Context ctx, String id) {
        if (entries.containsKey(id)) {
            ctx.json(entries.get(id));
        } else {
            ctx.status(404);
        }
    }

    @OpenApi(
            description = "Updates a ping",
            pathParams = {
                    @OpenApiParam(name = "id", description = "The id to be updated"),
            },
            responses = {
                    @OpenApiResponse(status = "200", content = { @OpenApiContent(type = Formats.JSON)}),
                    @OpenApiResponse(status = STATUS_404),
            },
            method = HttpMethod.PATCH,
            tags = {TAG}
    )
    public void update(@NotNull Context ctx, @NotNull String id) {
        if (entries.containsKey(id)) {
            entries.put(id, getNowStr());
            ctx.json(entries.get(id));
        } else {
            ctx.status(404);
        }
    }

    public static String getNowStr() {
        OffsetDateTime currentDateTimeWithOffset = OffsetDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z");
        return currentDateTimeWithOffset.format(formatter);
    }
}
