package cwms.cda;

import org.owasp.html.PolicyFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.openapi.OpenApiSchemeProcessor;
import io.javalin.config.Key;
import javax.sql.DataSource;

public final class CwmsDataApiAttributes {
    public static final Key<PolicyFactory> POLICY_FACTORY_KEY = new Key<>("PolicyFactory");;
    public static final Key<ObjectMapper> OBJECT_MAPPER_KEY = new Key<>("ObjectMapper");
    public static final Key<OpenApiSchemeProcessor> SCHEME_PROCESSOR_KEY = new Key<>("SchemeProcessor");
    public static final Key<DataSource> DATA_SOURCE_KEY = new Key<>("DataSource");
    public static final Key<String> OFFICE_ID_KEY = new Key<>("office");

    private CwmsDataApiAttributes() {
        /* utility class */
    }
}
