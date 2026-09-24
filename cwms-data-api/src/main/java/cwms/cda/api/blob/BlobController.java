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

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.BLOB_ID;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.CURSOR;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.LIKE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.queryParamAsClass;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.BaseCrudHandler;
import cwms.cda.api.RangeParser;
import cwms.cda.api.RangeRequestUtil;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.ExceptionTraceSupport;
import cwms.cda.data.dao.BlobAccess;
import cwms.cda.data.dao.BlobDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.ObjectStorageBlobDao;
import cwms.cda.data.dao.ObjectStorageConfig;
import cwms.cda.data.dao.StreamConsumer;
import cwms.cda.data.dto.Blob;
import cwms.cda.data.dto.Blobs;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.features.CdaFeatures;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import java.io.IOException;
import java.util.Optional;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.togglz.core.context.FeatureContext;
import org.togglz.core.manager.FeatureManager;

public abstract class BlobController extends BaseCrudHandler {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    static final int DEFAULT_PAGE_SIZE = 20;
    public static final String TAG = "Blob";

    protected BlobController(MetricRegistry metrics) {
        super(metrics);
    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    BlobAccess chooseBlobAccess(DSLContext dsl) {
        boolean useObjectStore = false;
        try {
            FeatureManager featureManager = FeatureContext.getFeatureManager();
            useObjectStore = featureManager.isActive(CdaFeatures.USE_OBJECT_STORAGE_BLOBS);
        } catch (Exception ignore) {
            // fall back to system/env property check
        }
        if (useObjectStore) {
            ObjectStorageConfig cfg = ObjectStorageConfig.fromSystem();
            return new ObjectStorageBlobDao(cfg);
        }
        return new BlobDao(dsl);
    }

    @Override
    public void getAll(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            String office = ctx.queryParam(OFFICE);

            String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                String.class, "", getMetrics(), name(BlobControllerV1.class.getName(), GET_ALL));

            if (!CwmsDTOPaginated.CURSOR_CHECK.invoke(cursor)) {
                ctx.json(new CdaError("cursor or page passed in but failed validation"))
                    .status(HttpCode.BAD_REQUEST);
                return;
            }

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE},
                Integer.class, DEFAULT_PAGE_SIZE, getMetrics(),
                name(BlobControllerV1.class.getName(), GET_ALL));

            String like = ctx.queryParamAsClass(LIKE, String.class).getOrDefault(".*");

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, Blobs.class);

            BlobAccess dao = chooseBlobAccess(dsl);
            Blobs blobs = dao.getBlobs(cursor, pageSize, office, like);

            String result = Formats.format(contentType, blobs);

            ctx.contentType(contentType.toString());
            updateResultSize(result.length());

            ctx.status(HttpServletResponse.SC_OK);

            byte[] bytes = result.getBytes();
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
            ctx.res.getOutputStream().write(bytes);
        } catch (IOException ex) {
            CdaError error = ExceptionTraceSupport.buildError(ctx, "Failed to process request to retrieve Blobs", ex);
            LOGGER.atSevere().withCause(ex).log("Failed to process request to retrieve Blobs");
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(error);
        }
    }

    @Override
    public void getOne(@NotNull Context ctx, @NotNull String blobId) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            String officeId = ctx.attribute(OFFICE);
            String idQueryParam = ctx.queryParam(BLOB_ID);
            if (idQueryParam != null) {
                blobId = idQueryParam;
            }
            DSLContext dsl = getDslContext(ctx);

            BlobAccess dao = chooseBlobAccess(dsl);
            Optional<String> office = Optional.ofNullable(officeId);


            final Long offset;
            final Long end;
            long[] ranges = RangeParser.parseFirstRange(ctx.header(io.javalin.core.util.Header.RANGE));
            if (ranges != null) {
                offset = ranges[0];
                end = ranges[1];
            } else {
                offset = null;
                end = null;
            }

            ctx.header(Header.ACCEPT_RANGES, "bytes");

            StreamConsumer consumer = (is, isPosition, mediaType, totalLength) -> {
                if (is == null) {
                    ctx.status(HttpServletResponse.SC_NOT_FOUND).json(new CdaError("Unable to find "
                        + "blob based on given parameters"));
                } else {
                    updateResultSize(totalLength);
                    // is  OracleBlobInputStream or something from MinIO
                    RangeRequestUtil.seekableStream(ctx, is, isPosition, mediaType, totalLength);
                }
            };

            if (office.isPresent()) {
                dao.getBlob(blobId, office.get(), consumer, offset, end);
            } else {
                dao.getBlob(blobId, null, consumer, offset, end);
            }
        }
    }

    @Override
    public void create(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);
            String formatHeader = ctx.req.getContentType();
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            ContentType contentType = Formats.parseHeader(formatHeader, Blob.class);
            Blob blob = Formats.parseContent(contentType, ctx.bodyAsInputStream(), Blob.class);
            BlobAccess dao = chooseBlobAccess(dsl);
            dao.create(blob, failIfExists, false);
            ctx.status(HttpCode.CREATED);
        }
    }

    @Override
    public void update(@NotNull Context ctx, @NotNull String blobId) {
        logUnusedPathParameter(ctx, BLOB_ID, "Body contains information");

        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            String idQueryParam = ctx.queryParam(BLOB_ID);
            if (idQueryParam != null) {
                blobId = idQueryParam;
            }
            DSLContext dsl = getDslContext(ctx);

            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSON;

            ContentType contentType = Formats.parseHeader(formatHeader, Blob.class);
            Blob blob = Formats.parseContent(contentType, ctx.bodyAsInputStream(), Blob.class);

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
                throw new FormattingException("The blob id parameter does not match the blob id in the body. " +
                    "The blob end-point does not support renaming blobs.  " +
                    "Create a new blob with the new id and delete the old one.");
            }

            BlobAccess dao = chooseBlobAccess(dsl);
            dao.update(blob, false);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    @Override
    public void delete(@NotNull Context ctx, @NotNull String blobId) {
        String office = ctx.attribute(OFFICE);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            String idQueryParam = ctx.queryParam(BLOB_ID);
            if (idQueryParam != null) {
                blobId = idQueryParam;
            }
            DSLContext dsl = getDslContext(ctx);
            BlobAccess dao = chooseBlobAccess(dsl);
            dao.delete(office, blobId);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
