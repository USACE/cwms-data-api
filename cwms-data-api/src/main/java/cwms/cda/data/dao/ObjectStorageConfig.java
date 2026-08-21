package cwms.cda.data.dao;

import java.util.Optional;

public class ObjectStorageConfig {

    private final String bucket;

    private final String endpoint;

    private final String accessKey;
    private final String secretKey;

    public ObjectStorageConfig(String bucket, String endpoint,
                               String accessKey, String secretKey) {

        this.bucket = bucket;

        this.endpoint = endpoint;

        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public static ObjectStorageConfig fromSystem() {

        String bucket = get("blob.store.bucket").orElse(null);

        String endpoint = get("blob.store.endpoint").orElse(null);

        String accessKey = get("blob.store.accessKey").orElse(null);
        String secretKey = get("blob.store.secretKey").orElse(null);
        return new ObjectStorageConfig(bucket, endpoint, accessKey, secretKey);
    }

    private static Optional<String> get(String key) {
        String sys = System.getProperty(key);
        if (sys != null && !sys.isEmpty()) return Optional.of(sys);
        String env = System.getenv(toEnvKey(key));
        if (env != null && !env.isEmpty()) return Optional.of(env);
        return Optional.empty();
    }

    private static String toEnvKey(String key) {
        return key.toUpperCase().replace('.', '_');
    }


    public String bucket() {
        return bucket;
    }

    public String endpoint() {
        return endpoint;
    }

    public String accessKey() {
        return accessKey;
    }

    public String secretKey() {
        return secretKey;
    }
}
