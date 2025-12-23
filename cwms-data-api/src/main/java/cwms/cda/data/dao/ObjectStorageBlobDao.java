package cwms.cda.data.dao;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.AlreadyExists;
import cwms.cda.api.errors.FieldLengthExceededException;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.Blob;
import cwms.cda.data.dto.Blobs;
import cwms.cda.data.dto.CwmsDTOPaginated;
import io.minio.*;
import io.minio.errors.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.sql.rowset.serial.SerialBlob;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

import io.minio.messages.Item;

/**
 * Object Storage-backed implementation using MinIO Java client.  keys like OFFICE/ID_UPPER.
 */
public class ObjectStorageBlobDao implements BlobAccess {
    FluentLogger logger = FluentLogger.forEnclosingClass();

    public static final int ID_LENGTH_LIMIT = 256;  // This is to match pl/sql limit
    private static final int MAX_KEY_LENGTH = 1024;
    private final ObjectStorageConfig config;
    private final MinioClient client;

    public ObjectStorageBlobDao(ObjectStorageConfig config) {
        this.config = config;
        this.client = buildClient(config);
    }

    private static MinioClient buildClient(ObjectStorageConfig cfg) {
        MinioClient.Builder b = MinioClient.builder();
        if (cfg.endpoint() != null && !cfg.endpoint().isEmpty()) {
            b = b.endpoint(cfg.endpoint());
        }
        if (cfg.accessKey() != null && cfg.secretKey() != null) {
            b = b.credentials(cfg.accessKey(), cfg.secretKey());
        }

        return b.build();
    }

    @Override
    public @NotNull Blobs getBlobs(@Nullable String cursor, int pageSize, @Nullable String officeId, @Nullable String like) {
        String prefix = null;
        if (officeId != null && !officeId.isEmpty()) {
            prefix = officeId.toUpperCase(Locale.ROOT) + "/";
        }

        String startAfter = null;

        String cursorOffice = null;
        String cursorId = null;
        if (cursor != null && !cursor.isEmpty()) {
            final String[] parts = CwmsDTOPaginated.decodeCursor(cursor, "||");

            if (parts.length > 1) {
                cursorOffice = Blobs.getOffice(cursor);
                cursorId = Blobs.getId(cursor);
                pageSize = Integer.parseInt(parts[2]);
            }

            if (cursorOffice != null && cursorId != null) {
                startAfter = key(cursorOffice, cursorId);
            }
        }


        Pattern likePattern = null;
        if (like != null && !like.isEmpty() && !".*".equals(like)) {
            likePattern = Pattern.compile(like, Pattern.CASE_INSENSITIVE);
        }

        List<Blob> collected = new ArrayList<>();

        ListObjectsArgs.Builder args = ListObjectsArgs.builder()
                .bucket(requiredBucket())
                .recursive(true)
                .maxKeys(pageSize);
        if (prefix != null) args = args.prefix(prefix);
        if (startAfter != null) args = args.startAfter(startAfter);

        for (Result<Item> res : client.listObjects(args.build())) {
            try {
                // item.key() like OFFICE/ID
                Item item = res.get();
                String k = item.objectName();
                int slash = k.indexOf('/');
                if (slash <= 0 || slash >= k.length() - 1) continue;
                String off = k.substring(0, slash);
                String id = k.substring(slash + 1);
                if (likePattern != null && !likePattern.matcher(id).find()) {
                    continue;
                }
                // fetch metadata for media type and optional description
                try {
                    StatObjectResponse stat = client.statObject(StatObjectArgs.builder()
                            .bucket(requiredBucket())
                            .object(k)
                            .build());
                    String mediaType = stat.contentType();
                    String desc = stat.userMetadata() != null ? stat.userMetadata().getOrDefault("description", null) : null;
                    collected.add(new Blob(off, id, desc, mediaType, null));
                    if (collected.size() >= pageSize) break;
                } catch (Exception e) {
                    // skip items that fail stat
                }
            } catch (Exception ignore) {
                // skip this entry on error
            }
        }

        Blobs.Builder builder = new Blobs.Builder(cursor, pageSize, 0);
        collected.forEach(builder::addBlob);
        return builder.build();
    }

    @Override
    public Optional<Blob> getByUniqueName(String id, String office) {
        String k = (office == null || office.isEmpty()) ? findFirstKeyById(id) : key(office, id);
        if (k == null) {
            return Optional.empty();
        }
        String officeFromKey = officeFromKey(k);
        String idFromKey = idFromKey(k);
        try {
            StatObjectResponse stat = client.statObject(StatObjectArgs.builder()
                    .bucket(requiredBucket())
                    .object(k)
                    .build());
            String mediaType = stat.contentType();
            String desc = stat.userMetadata() != null ? stat.userMetadata().getOrDefault("description", null) : null;
            return Optional.of(new Blob(officeFromKey, idFromKey, desc, mediaType, null));
        } catch (ErrorResponseException ere) {
            if ("NoSuchKey".equalsIgnoreCase(ere.errorResponse().code())) {
                return Optional.empty();
            }
            throw new RuntimeException(ere);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void getBlob(String id, String office, BlobDao.BlobConsumer consumer) {
        String k = (office == null || office.isEmpty()) ? findFirstKeyById(id) : key(office, id);
        try {
            if (k == null) {
                try {
                    consumer.accept(null, null);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return;
            }
            logger.atFine().log("Getting stat for %s", k);
            // Stat first to get content type and size
            StatObjectResponse stat = client.statObject(StatObjectArgs.builder()
                    .bucket(requiredBucket())
                    .object(k)
                    .build());
            String mediaType = stat.contentType() != null ? stat.contentType() : "application/octet-stream";

            try (InputStream is = client.getObject(GetObjectArgs.builder()
                    .bucket(requiredBucket())
                    .object(k)
                    .build())) {
                // Its too bad this has to readFully  - future optimization can skip ahead
                // b/c the consumer really just wants to get the stream out of the blob.
                byte[] data = readFully(is);
                SerialBlob blob = new SerialBlob(data);
                consumer.accept(blob, mediaType);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } catch (ErrorResponseException ere) {
            if ("NoSuchKey".equalsIgnoreCase(ere.errorResponse().code())) {
                try {
                    // We could also just throw a NotFoundException.
                    // BlobController suggests consumer.accept(null, null); will handle things.
                    consumer.accept(null, null);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return;
            }
            throw new RuntimeException(ere);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void create(Blob blob, boolean failIfExists, boolean ignoreNulls) {
        String k = key(blob.getOfficeId(), blob.getId());
        if (failIfExists) {
            try {
                client.statObject(StatObjectArgs.builder()
                        .bucket(requiredBucket())
                        .object(k)
                        .build());
                throw new AlreadyExists("Blob already exists: " + k, null);
            } catch (ErrorResponseException ere) {
                if (!"NoSuchKey".equalsIgnoreCase(ere.errorResponse().code())) {
                    throw new RuntimeException(ere);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // TODO: Figure out which of these can be something better.
        try {
            doPut(blob, k, ignoreNulls);
        } catch (ServerException e) {
            throw new RuntimeException(e);
        } catch (InsufficientDataException e) {
            throw new RuntimeException(e);
        } catch (ErrorResponseException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (InvalidKeyException e) {
            throw new RuntimeException(e);
        } catch (InvalidResponseException e) {
            throw new RuntimeException(e);
        } catch (XmlParserException e) {
            throw new RuntimeException(e);
        } catch (InternalException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void update(Blob blob, boolean ignoreNulls) {
        String k = key(blob.getOfficeId(), blob.getId());
        // For updatemake sure it exists first
        try {
            client.statObject(StatObjectArgs.builder()
                    .bucket(requiredBucket())
                    .object(k)
                    .build());
        } catch (ErrorResponseException ere) {
            if ("NoSuchKey".equalsIgnoreCase(ere.errorResponse().code())) {
                throw new NotFoundException("Unable to find blob with id " + blob.getId() + " in office " + blob.getOfficeId());
            }
            throw new RuntimeException(ere);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            doPut(blob, k, ignoreNulls);
        } catch (ServerException e) {
            throw new RuntimeException(e);
        } catch (InsufficientDataException e) {
            throw new RuntimeException(e);
        } catch (ErrorResponseException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (InvalidKeyException e) {
            throw new RuntimeException(e);
        } catch (InvalidResponseException e) {
            throw new RuntimeException(e);
        } catch (XmlParserException e) {
            throw new RuntimeException(e);
        } catch (InternalException e) {
            throw new RuntimeException(e);
        }
    }

    private void doPut(Blob blob, String k, boolean ignoreNulls) throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        byte[] value = blob.getValue();
        if (value == null && ignoreNulls) {
            return;
        }

        if (value == null) {
            value = new byte[0];
        }

        try (InputStream is = new ByteArrayInputStream(value)) {
            PutObjectArgs.Builder builder = PutObjectArgs.builder()
                    .bucket(requiredBucket())
                    .object(k)
                    .stream(is, value.length, -1)
                    .contentType(blob.getMediaTypeId());

            if (blob.getDescription() != null) {
                builder.userMetadata(java.util.Collections.singletonMap("description", blob.getDescription()));
            }

            client.putObject(builder.build());
        }
    }

    @Override
    public void delete(String office, String id) {
        String k = key(office, id);
        try {
            client.removeObject(RemoveObjectArgs.builder()
                    .bucket(requiredBucket())
                    .object(k)
                    .build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String findFirstKeyById(String id) {
        String targetSuffix = "/" + normalizeId(id).toUpperCase(Locale.ROOT);
        try {
            ListObjectsArgs args = ListObjectsArgs.builder()
                    .bucket(requiredBucket())
                    .recursive(true)
                    .build();
            for (Result<Item> res : client.listObjects(args)) {
                try {
                    Item item = res.get();
                    String name = item.objectName();
                    if (name.toUpperCase(Locale.ROOT).endsWith(targetSuffix)) {
                        return name;
                    }
                } catch (Exception ignore) {
                }
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String officeFromKey(String key) {
        int slash = key.indexOf('/');
        return (slash > 0) ? key.substring(0, slash) : null;
    }

    private static String idFromKey(String key) {
        int slash = key.indexOf('/');
        return (slash >= 0 && slash < key.length() - 1) ? key.substring(slash + 1) : key;
    }

    private String requiredBucket() {
        String bucket = config.bucket();
        if (bucket == null || bucket.isEmpty()) {
            throw new IllegalStateException("Object storage bucket is not configured (blob.store.bucket)");
        }
        return bucket;
    }

    private static String key(String office, String id) {
        String off = office == null ? "" : office.toUpperCase(Locale.ROOT);
        String nid = normalizeId(id).toUpperCase(Locale.ROOT);
        String fullKey = off + "/" + nid;
        if (fullKey.length() > MAX_KEY_LENGTH) {
            throw new FieldLengthExceededException("Key", fullKey.length(), MAX_KEY_LENGTH, null, true);
        }
        return fullKey;
    }

    private static String normalizeId(String id) {
        if (id == null) return "";

        if(id.length() > ID_LENGTH_LIMIT){
            throw new FieldLengthExceededException("ID", id.length(), ID_LENGTH_LIMIT, null, true);
        }
        // Replace spaces with underscore; leave common safe chars; percent-encode others
        StringBuilder sb = new StringBuilder();
        for (char c : id.toCharArray()) {
            if (Character.isLetterOrDigit(c) || c == '.' || c == '_' || c == '-' ) {
                sb.append(c);
            } else if (c == ' ') {
                sb.append('_');
            } else if (c == '/') {
                // keep slash because controller may pass IDs containing '/'; since we prefix with OFFICE/, this would nest more levels
                sb.append('/');
            } else {
                String hex = Integer.toHexString(c).toUpperCase(Locale.ROOT);
                if (hex.length() == 1) hex = "0" + hex;
                sb.append('%').append(hex);
            }
        }
        return sb.toString();
    }

    private static byte[] readFully(InputStream is) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int r;
        while ((r = is.read(buf)) != -1) {
            baos.write(buf, 0, r);
        }
        return baos.toByteArray();
    }
}
