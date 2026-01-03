package cwms.cda.data.dao;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.RangeParser;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
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
    public static final String DESCRIPTION = "description";
    public static final String NO_SUCH_KEY = "NoSuchKey";
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
                startAfter = buildName(cursorOffice, cursorId);
            }
        }

        Pattern likePattern = null;
        if (like != null && !like.isEmpty() && !".*".equals(like)) {
            likePattern = Pattern.compile(like, Pattern.CASE_INSENSITIVE);
        }
        List<Blob> collected = getBlobs(pageSize, likePattern, prefix, startAfter);

        Blobs.Builder builder = new Blobs.Builder(cursor, pageSize, 0);
        collected.forEach(builder::addBlob);
        return builder.build();
    }

    private @NotNull List<Blob> getBlobs(int pageSize, @Nullable Pattern likePattern, String prefix, String startAfter) {
        List<Blob> collected = new ArrayList<>();

        ListObjectsArgs.Builder args = ListObjectsArgs.builder()
                .bucket(requiredBucket())
                .recursive(true)
                .maxKeys(pageSize);
        if (prefix != null) {
            args = args.prefix(prefix);
        }
        if (startAfter != null){
            args = args.startAfter(startAfter);
        }

        for (Result<Item> res : client.listObjects(args.build())) {
            try {
                // item.key() like OFFICE/ID
                Item item = res.get();
                String name = item.objectName();
                if(nameMatches(name, likePattern)) {
                    try {
                        Blob blob = getBlob(name);
                        collected.add(blob);
                        if (collected.size() >= pageSize) {
                            break;
                        }
                    } catch (Exception e) {
                        // skip items that fail stat
                    }
                }
            }
            catch (Exception ignore) {
                // skip this entry on error
            }
        }
        return collected;
    }

    private @NotNull Blob getBlob(String name) throws ErrorResponseException, InsufficientDataException, InternalException, InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException, ServerException, XmlParserException {
        StatObjectResponse stat = client.statObject(StatObjectArgs.builder()
                .bucket(requiredBucket())
                .object(name)
                .build());
        String mediaType = stat.contentType();
        String desc = stat.userMetadata() != null ? stat.userMetadata().getOrDefault(DESCRIPTION, null) : null;
        return new Blob(officeFromName(name), idFromName(name), desc, mediaType, null);
    }

    public static String officeFromName(String k){
        String off = null;
        int slash = k.indexOf('/');
        if (slash > 0 && slash < k.length() - 1) {
            off = k.substring(0, slash);
        }
        return off;
    }

    public static String idFromName(String k) {
        String id = null;
        int slash = k.indexOf('/');
        if (slash > 0 && slash < k.length() - 1) {
            id = k.substring(slash + 1);
        }
        return id;
    }
    
    public static boolean nameMatches(String name, Pattern likePattern) {
        boolean nameMatches = false;

        int slash = name.indexOf('/');
        if (slash > 0 && slash < name.length() - 1) {
            String id = name.substring(slash + 1);
            if (likePattern == null || likePattern.matcher(id).find()) {
                nameMatches = true;
            }
        } 
        return nameMatches;
    }
    
    
    @Override
    public Optional<Blob> getByUniqueName(String id, String office) {
        String k = (office == null || office.isEmpty()) ? findFirstKeyById(id) : buildName(office, id);
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
            String desc = stat.userMetadata() != null ? stat.userMetadata().getOrDefault(DESCRIPTION, null) : null;
            return Optional.of(new Blob(officeFromKey, idFromKey, desc, mediaType, null));
        } catch (ErrorResponseException ere) {
            if (NO_SUCH_KEY.equalsIgnoreCase(ere.errorResponse().code())) {
                return Optional.empty();
            }
            throw new RuntimeException(ere);
        } catch (ServerException | InternalException | XmlParserException | InvalidResponseException |
                 InvalidKeyException | NoSuchAlgorithmException | IOException | InsufficientDataException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void getBlob(String id, String office, StreamConsumer consumer, @Nullable Long offset, @Nullable Long end) {
        String key = (office == null || office.isEmpty()) ? findFirstKeyById(id) : buildName(office, id);
        if (key == null) {
            throw new NotFoundException("Could not find blob with id:" + id + " in office:" + office);
        }
        try {
            logger.atFine().log("Getting stat for %s", key);

            StatObjectResponse stat = client.statObject(StatObjectArgs.builder()
                    .bucket(requiredBucket())
                    .object(key)
                    .build());
            String mediaType = stat.contentType() != null ? stat.contentType() : "application/octet-stream";
            long totalLength = stat.size();
            
            streamToConsumer(key, consumer, offset, end, mediaType, totalLength);
        } catch (ErrorResponseException ere) {
            if (NO_SUCH_KEY.equalsIgnoreCase(ere.errorResponse().code())) {
                throw new NotFoundException("Could not find blob with id:" + id + " in office:" + office);
            }
            throw new RuntimeException(ere);
        } catch (ServerException | InternalException | XmlParserException | InvalidResponseException |
                 InvalidKeyException | NoSuchAlgorithmException | IOException | InsufficientDataException |
                 SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void streamToConsumer(String name, StreamConsumer consumer, @Nullable Long offset, @Nullable Long end,
                                  String mediaType, long totalLength) throws SQLException, IOException {

        if(offset != null && end != null){
            long[] startEnd = RangeParser.interpret(new long[]{offset, end}, totalLength);
            offset = startEnd[0];
            end = startEnd[1];
        }

        GetObjectArgs.Builder builder = GetObjectArgs.builder()
                .bucket(requiredBucket())
                .object(name);
        if(offset != null ) {
            builder = builder.offset(offset);
        } else {
            offset = 0L;
        }

        if(end != null && end > 0) {
            long length = end - offset + 1;
            builder = builder.length(length);
        }

        try (InputStream is = client.getObject(builder.build())) {
            consumer.accept(is, offset, mediaType, totalLength);
        } catch (ServerException | InsufficientDataException e) {
            throw new IOException(e);
        } catch (InvalidKeyException e) {
            throw new NotFoundException(e);
        } catch (ErrorResponseException | NoSuchAlgorithmException | InvalidResponseException | XmlParserException |
                 InternalException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void create(Blob blob, boolean failIfExists, boolean ignoreNulls)  {
        String name = buildName(blob.getOfficeId(), blob.getId());
        if (failIfExists) {
            try {
                client.statObject(StatObjectArgs.builder()
                        .bucket(requiredBucket())
                        .object(name)
                        .build());
                throw new AlreadyExists("Blob already exists: " + name, null);
            } catch (ErrorResponseException ere) {
                if (!NO_SUCH_KEY.equalsIgnoreCase(ere.errorResponse().code())) {
                    throw new RuntimeException(ere);
                }
            } catch (ServerException | InsufficientDataException | IOException | NoSuchAlgorithmException |
                     InternalException | XmlParserException | InvalidResponseException | InvalidKeyException e) {
                throw new RuntimeException(e);
            }
        }
    
        try {
            doPut(blob, name, ignoreNulls);
        } catch (ServerException | InsufficientDataException | ErrorResponseException | NoSuchAlgorithmException |
                 InvalidKeyException | InvalidResponseException | XmlParserException | InternalException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void update(Blob blob, boolean ignoreNulls) {
        String name = buildName(blob.getOfficeId(), blob.getId());
        // For update make sure it exists first
        try {
            client.statObject(StatObjectArgs.builder()
                    .bucket(requiredBucket())
                    .object(name)
                    .build());
            doPut(blob, name, ignoreNulls);
        } catch (ErrorResponseException ere) {
            if (NO_SUCH_KEY.equalsIgnoreCase(ere.errorResponse().code())) {
                throw new NotFoundException("Unable to find blob with id " + blob.getId() + " in office " + blob.getOfficeId());
            }
            throw new RuntimeException(ere);
        } catch (ServerException | IOException | InsufficientDataException | NoSuchAlgorithmException |
                 InvalidKeyException | InvalidResponseException | XmlParserException | InternalException e) {
            throw new RuntimeException(e);
        }
    }

    private void doPut(Blob blob, String name, boolean ignoreNulls) throws ServerException, InsufficientDataException, ErrorResponseException, IOException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
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
                    .object(name)
                    .stream(is, value.length, -1)
                    .contentType(blob.getMediaTypeId());

            if (blob.getDescription() != null) {
                builder.userMetadata(java.util.Collections.singletonMap(DESCRIPTION, blob.getDescription()));
            }

            client.putObject(builder.build());
        }
    }

    @Override
    public void delete(String office, String id) {
        String name = buildName(office, id);
        try {
            client.removeObject(RemoveObjectArgs.builder()
                    .bucket(requiredBucket())
                    .object(name)
                    .build());
        } catch (ServerException | XmlParserException | ErrorResponseException | InsufficientDataException |
                 IOException | NoSuchAlgorithmException | InvalidKeyException | InvalidResponseException |
                 InternalException e) {
            throw new RuntimeException(e);
        }
    }

    private String findFirstKeyById(String id) {
        String targetSuffix = "/" + normalizeId(id).toUpperCase(Locale.ROOT);

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
            } catch (ErrorResponseException | InsufficientDataException | XmlParserException | ServerException |
                     NoSuchAlgorithmException | IOException | InvalidResponseException | InvalidKeyException |
                     InternalException e) {
                throw new RuntimeException(e);
            }

        }
        return null;
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

    private static String buildName(String office, String id) {
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

   
}
