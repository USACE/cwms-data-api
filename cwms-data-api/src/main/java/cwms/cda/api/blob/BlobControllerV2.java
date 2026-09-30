/*
 *
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.blob;

import static cwms.cda.api.Controllers.BLOB_ID;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.CURSOR;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.LIKE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.MultipartParser.parseMultipart;
import static cwms.cda.api.MultipartParser.readMultipartValue;
import static cwms.cda.formatters.Formats.MULTIPART_FORM_DATA;
import static org.apache.commons.lang3.StringUtils.firstNonBlank;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.MultipartParser;
import cwms.cda.data.dao.BlobAccess;
import cwms.cda.data.dto.Blob;
import cwms.cda.data.dto.Blobs;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.Locale;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class BlobControllerV2 extends BlobController {
    public BlobControllerV2(MetricRegistry metrics) {
        super(metrics);
    }

    @OpenApi(
        description = "Create new Blob",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = Blob.class, type = Formats.JSON),
                @OpenApiContent(from = Blob.class, type = MULTIPART_FORM_DATA)
            },
            required = true),
        queryParams = {
            @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                description = "Create will fail if provided ID already exists. Default: true")
        },
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office.")
        },
        method = HttpMethod.POST,
        tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            String office = ctx.pathParam(OFFICE);
            DSLContext dsl = getDslContext(ctx);
            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSON;
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);

            Blob blob;
            if (formatHeader.toLowerCase(Locale.ROOT).startsWith(MULTIPART_FORM_DATA)) {
                blob = parseMultipartBlob(ctx, office, null);
            } else {
                ContentType contentType = Formats.parseHeader(formatHeader, Blob.class);
                blob = Formats.parseContent(contentType, ctx.bodyAsInputStream(), Blob.class);
            }
            BlobAccess dao = chooseBlobAccess(dsl);
            dao.create(blob, failIfExists, false);
            ctx.status(HttpCode.CREATED);
        }
    }

    @OpenApi(
        description = "Update an existing Blob",
        pathParams = {
            @OpenApiParam(name = BLOB_ID, description = "The blob identifier to be updated"),
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office.")
        },
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = Blob.class, type = Formats.JSON),
                @OpenApiContent(from = Blob.class, type = MULTIPART_FORM_DATA)
            },
            required = true),
        queryParams = {
            @OpenApiParam(name = BLOB_ID, required = false,
                description = "If this _query_ parameter is provided the id _path_ parameter "
                + "is ignored and the value of the query parameter is used.   "
                + "Note: this query parameter is necessary for id's that contain '/' or other special "
                + "characters. This is due to limitations in path pattern matching. "
                + "We will likely add support for encoding the ID in the path in the future. "
                + "For now use the id field for those IDs. "
                + "Client libraries should detect slashes and choose the appropriate field. "
                + "\"ignored\" is suggested for the path endpoint."),
        },
        method = HttpMethod.PATCH,
        tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String blobId) {
        ctx.pathParam(BLOB_ID); // included for openapi tests to recognize the path param as in-use
        String office = ctx.pathParam(OFFICE);
        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            String idQueryParam = ctx.queryParam(BLOB_ID);
            if (idQueryParam != null) {
                blobId = idQueryParam;
            }

            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSON;

            Blob blob;
            if (formatHeader.toLowerCase(Locale.ROOT).startsWith(MULTIPART_FORM_DATA)) {
                blob = parseMultipartBlob(ctx, office, blobId);
            } else {
                ContentType contentType = Formats.parseHeader(formatHeader, Blob.class);
                blob = Formats.parseContent(contentType, ctx.bodyAsInputStream(), Blob.class);
            }

            if (blob.getOfficeId() == null) {
                throw new FormattingException("An officeId is required when updating a blob");
            }

            if (blob.getId() == null) {
                throw new FormattingException("An Id is required when updating a blob");
            }

            if (blob.getValue() == null) {
                throw new FormattingException("A non-empty value field is required when "
                    + "updating a blob");
            }

            if (!blob.getId().equals(blobId)) {
                throw new FormattingException("The blob id parameter does not match the blob id in the body. "
                    + "The blob end-point does not support renaming blobs.  "
                    + "Create a new blob with the new id and delete the old one.");
            }

            DSLContext dsl = getDslContext(ctx);
            BlobAccess dao = chooseBlobAccess(dsl);
            dao.update(blob, false);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    @OpenApi(
        description = "Deletes requested blob",
        pathParams = {
            @OpenApiParam(name = BLOB_ID, description = "The blob identifier to be deleted"),
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office.")
        },
        queryParams = {
            @OpenApiParam(name = BLOB_ID, required = false,
                description = "If this _query_ parameter is provided the id _path_ parameter "
                + "is ignored and the value of the query parameter is used.   "
                + "Note: this query parameter is necessary for id's that contain '/' or other special "
                + "characters. This is due to limitations in path pattern matching. "
                + "We will likely add support for encoding the ID in the path in the future. "
                + "For now use the id field for those IDs. "
                + "Client libraries should detect slashes and choose the appropriate field. "
                + "\"ignored\" is suggested for the path endpoint."),
        },
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String blobId) {
        String office = ctx.pathParam(OFFICE);
        ctx.pathParam(BLOB_ID); // included for openapi tests to recognize the path param as in-use
        ctx.attribute(OFFICE, office);
        super.delete(ctx, blobId);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE,
                description = "Specifies the owning office. If this field is not "
                    + "specified, matching information from all offices shall be "
                    + "returned."),
            @OpenApiParam(name = PAGE,
                description = "This end point can return a lot of data, this "
                    + "identifies where in the request you are. This is an opaque"
                    + " value, and can be obtained from the 'next-page' value in "
                    + "the response."),
            @OpenApiParam(name = CURSOR, deprecated = true,
                description = "This end point can return a lot of data, this "
                    + "identifies where in the request you are. This is an opaque"
                    + " value, and can be obtained from the 'next-page' value in "
                    + "the response. Deprecated, use " + PAGE + " instead."),
            @OpenApiParam(name = PAGE_SIZE,
                type = Integer.class,
                description = "How many entries per page returned. Default "
                    + DEFAULT_PAGE_SIZE + "."),
            @OpenApiParam(name = LIKE,
                description = "Posix <a href=\"regexp.html\">regular expression</a> "
                    + "describing the blob id's you want")
        },
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office.")
        },
        responses = {@OpenApiResponse(status = STATUS_200,
            description = "A list of blobs.",
            content = {
                @OpenApiContent(type = Formats.JSON, from = Blobs.class),
                @OpenApiContent(type = Formats.JSONV2, from = Blobs.class),
            })
        },
        tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        logUnusedPathParameter(ctx, OFFICE, "Query parameter used instead");
        super.getAll(ctx);
    }

    @OpenApi(
        description = "Returns the binary value of the requested blob as a seekable stream with the "
            + "appropriate media type.",
        pathParams = {
            @OpenApiParam(name = BLOB_ID, description = "If the _query_ parameter is provided this _path_ parameter "
                + "is ignored and the value of the query parameter is used.   "
                + "Note: the _query_ parameter is necessary for id's that contain '/' or other special "
                + "characters. This is due to limitations in path pattern matching. "
                + "We will likely add support for encoding the ID in the path in the future. "
                + "For now use the id field for those IDs. "
                + "Client libraries should detect slashes and choose the appropriate field. "
                + "\"ignored\" is suggested for the path endpoint."),
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office.")
        },
        queryParams = {
            @OpenApiParam(name = BLOB_ID, required = false,
                description = "If this _query_ parameter is provided the id _path_ parameter "
                + "is ignored and the value of the query parameter is used.   "
                + "Note: this query parameter is necessary for id's that contain '/' or other special "
                + "characters. This is due to limitations in path pattern matching. "
                + "We will likely add support for encoding the ID in the path in the future."
                + " For now use the id field for those IDs. "
                + "Client libraries should detect slashes and choose the appropriate field."
                + " \"ignored\" is suggested for the path endpoint.")
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                description = "Returns requested blob.",
                content = {
                    @OpenApiContent(type = "application/octet-stream", from = byte[].class)
                })
        },
        tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String blobId) {
        String office = ctx.pathParam(OFFICE);
        ctx.pathParam(BLOB_ID); // included for openapi tests to recognize the path param as in-use
        ctx.attribute(OFFICE, office);
        super.getOne(ctx, blobId);
    }

    private Blob parseMultipartBlob(Context ctx, String office, String blobId) {
        MultipartParser.ParsedMultipart parsedMultipart = parseMultipart(ctx);

        String id = firstNonBlank(blobId, parsedMultipart.field("id"));
        String description = parsedMultipart.field("description");
        String mediaTypeId = parsedMultipart.field("media-type-id");

        byte[] value = parsedMultipart.value();
        if (value == null) {
            value = readMultipartValue(ctx);
        }
        return new Blob(office, id, description, mediaTypeId, value);
    }
}
