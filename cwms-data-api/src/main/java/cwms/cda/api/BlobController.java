package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;

import static cwms.cda.api.Controllers.*;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.BlobDao;
import cwms.cda.data.dao.BlobAccess;
import cwms.cda.data.dao.ObjectStorageBlobDao;
import cwms.cda.data.dao.ObjectStorageConfig;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.Blob;
import cwms.cda.data.dto.Blobs;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.InputStream;
import java.util.Optional;

import javax.servlet.http.HttpServletResponse;

import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.togglz.core.context.FeatureContext;
import cwms.cda.features.CdaFeatures;
import org.togglz.core.manager.FeatureManager;


/**
 *
 */
public class BlobController implements CrudHandler {
    private static final int DEFAULT_PAGE_SIZE = 20;
    public static final String TAG = "Blob";

    private final MetricRegistry metrics;


    private final Histogram requestResultSize;

    public BlobController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = BlobController.class.getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    private BlobAccess chooseBlobAccess(DSLContext dsl) {
        boolean useObjectStore = isObjectStorageEnabled();
        try {
            // Prefer Togglz if available
            FeatureManager featureManager = FeatureContext.getFeatureManager();
            useObjectStore = featureManager.isActive(CdaFeatures.USE_OBJECT_STORAGE_BLOBS);
        } catch (Throwable ignore) {
            // fall back to system/env property check
        }
        if (useObjectStore) {
            ObjectStorageConfig cfg = ObjectStorageConfig.fromSystem();
            return new ObjectStorageBlobDao(cfg);
        }
        return new BlobDao(dsl);
    }

    private boolean isObjectStorageEnabled() {
        // System properties first, then env. Accept FEATURE=true
        String key = String.valueOf(CdaFeatures.USE_OBJECT_STORAGE_BLOBS);
        String v = System.getProperty(key);
        if (v == null) v = System.getProperty(key);
        if (v == null) v = System.getenv(key);
        return v != null && ("true".equalsIgnoreCase(v) || "1".equals(v));
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
            @OpenApiParam(name = PAGE_SIZE,
                type = Integer.class,
                description = "How many entries per page returned. Default "
                        + DEFAULT_PAGE_SIZE + "."),
            @OpenApiParam(name = LIKE,
                description = "Posix <a href=\"regexp.html\">regular expression</a> "
                        + "describing the blob id's you want")
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

        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            String office = ctx.queryParam(OFFICE);

            String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                    String.class, "", metrics, name(BlobController.class.getName(), GET_ALL));

            if (!CwmsDTOPaginated.CURSOR_CHECK.invoke(cursor)) {
                ctx.json(new CdaError("cursor or page passed in but failed validation"))
                        .status(HttpCode.BAD_REQUEST);
                return;
            }

            int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE},
                    Integer.class, DEFAULT_PAGE_SIZE, metrics,
                    name(BlobController.class.getName(), GET_ALL));

            String like = ctx.queryParamAsClass(LIKE, String.class).getOrDefault(".*");

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, Blobs.class);

            BlobAccess dao = chooseBlobAccess(dsl);
            Blobs blobs = dao.getBlobs(cursor, pageSize, office, like);

            String result = Formats.format(contentType, blobs);

            ctx.result(result);
            ctx.contentType(contentType.toString());
            requestResultSize.update(result.length());
        }
    }

    @OpenApi(
            description = "Returns the binary value of the requested blob as a seekable stream with the "
                    + "appropriate media type.",
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the owning office."),
                @OpenApiParam(name = BLOB_ID, description = "If this _query_ parameter is provided the id _path_ parameter "
                    + "is ignored and the value of the query parameter is used.   "
                    + "Note: this query parameter is necessary for id's that contain '/' or other special "
                    + "characters. This is due to limitations in path pattern matching. "
                    + "We will likely add support for encoding the ID in the path in the future. For now use the id field for those IDs. "
                    + "Client libraries should detect slashes and choose the appropriate field. \"ignored\" is suggested for the path endpoint."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200,
                    description = "Returns requested blob.",
                    content = {
                        @OpenApiContent(type = "application/octet-stream")
                    })
            },
            tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String blobId) {

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            String idQueryParam = ctx.queryParam(BLOB_ID);
            if (idQueryParam != null) {
                blobId = idQueryParam;
            }
            DSLContext dsl = getDslContext(ctx);

            BlobAccess dao = chooseBlobAccess(dsl);
            String officeQP = ctx.queryParam(OFFICE);
            Optional<String> office = Optional.ofNullable(officeQP);

            BlobDao.BlobConsumer tripleConsumer = (blob, mediaType) -> {

                if (blob == null) {
                    ctx.status(HttpServletResponse.SC_NOT_FOUND).json(new CdaError("Unable to find "
                            + "blob based on given parameters"));
                } else {
                    long size = blob.length();
                    requestResultSize.update(size);
                    try (InputStream is = blob.getBinaryStream()) { // is  OracleBlobInputStream
                        RangeRequestUtil.seekableStream(ctx, is, mediaType, size);
                    }
                }
            };
            if (office.isPresent()) {
                dao.getBlob(blobId, office.get(), tripleConsumer);
            } else {
                dao.getBlob(blobId, tripleConsumer);
            }
        }
    }


    @OpenApi(
            description = "Create new Blob",
            requestBody = @OpenApiRequestBody(
                content = {
                    @OpenApiContent(from = Blob.class, type = Formats.JSONV2)
                },
                required = true),
            queryParams = {
                @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                    description = "Create will fail if provided ID already exists. Default: true")
            },
            method = HttpMethod.POST,
            tags = {TAG}
    )
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

    @OpenApi(
            description = "Update an existing Blob",
            pathParams = {
                @OpenApiParam(name = BLOB_ID, description = "The blob identifier to be updated"),
            },
            requestBody = @OpenApiRequestBody(
                content = {
                    @OpenApiContent(from = Blob.class, type = Formats.JSONV2),
                    @OpenApiContent(from = Blob.class, type = Formats.JSON)
                },
                required = true),
            queryParams = {
                @OpenApiParam(name = BLOB_ID, description = "If this _query_ parameter is provided the id _path_ parameter "
                    + "is ignored and the value of the query parameter is used.   "
                    + "Note: this query parameter is necessary for id's that contain '/' or other special "
                    + "characters. This is due to limitations in path pattern matching. "
                    + "We will likely add support for encoding the ID in the path in the future. For now use the id field for those IDs. "
                    + "Client libraries should detect slashes and choose the appropriate field. \"ignored\" is suggested for the path endpoint."),
            },
            method = HttpMethod.PATCH,
            tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String blobId) {
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

            if(!blob.getId().equals(blobId)) {
                throw new FormattingException("The blob id parameter does not match the blob id in the body. " +
                        "The blob end-point does not support renaming blobs.  " +
                        "Create a new blob with the new id and delete the old one.");
            }

            BlobAccess dao = chooseBlobAccess(dsl);
            dao.update(blob, false);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    @OpenApi(
            description = "Deletes requested blob",
            pathParams = {
                @OpenApiParam(name = BLOB_ID, description = "The blob identifier to be deleted"),
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                    + "owning office of the blob to be deleted"),
                @OpenApiParam(name = BLOB_ID, description = "If this _query_ parameter is provided the id _path_ parameter "
                    + "is ignored and the value of the query parameter is used.   "
                    + "Note: this query parameter is necessary for id's that contain '/' or other special "
                    + "characters. This is due to limitations in path pattern matching. "
                    + "We will likely add support for encoding the ID in the path in the future. For now use the id field for those IDs. "
                    + "Client libraries should detect slashes and choose the appropriate field. \"ignored\" is suggested for the path endpoint."),
            },
            method = HttpMethod.DELETE,
            tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String blobId) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            String idQueryParam = ctx.queryParam(BLOB_ID);
            if (idQueryParam != null) {
                blobId = idQueryParam;
            }
            DSLContext dsl = getDslContext(ctx);
            String office = requiredParam(ctx, OFFICE);
            BlobAccess dao = chooseBlobAccess(dsl);
            dao.delete(office, blobId);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
