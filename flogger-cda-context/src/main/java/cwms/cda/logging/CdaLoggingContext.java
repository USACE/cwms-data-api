package cwms.cda.logging;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.MDC;

import com.google.common.flogger.MetadataKey;
import com.google.common.flogger.context.ScopeType;
import com.google.common.flogger.context.ScopedLoggingContext;

/**
 * MAP Flogger Scoped MetaData to SLF4J MDC to apply to the MDC section of the JSON log.
 * Default implementation against JUL (with SLF4j or Flogger) will not show any MDC values.
 *
 * NOTE: This all assumes SLF4j is somewhere in the mix and used in the logging chain.
 *
 * Metadata keys and Tags are treated the same. Object::toString is used to convert everything to a string.
 */
public class CdaLoggingContext extends ScopedLoggingContext {
    public static final MetadataKey<String> TRACE_ID = MetadataKey.single("traceId", String.class);

    @Override
    public Builder newContext() {
        return new CdaBuilder();
    }

    @Override
    public Builder newContext(ScopeType scopeType) {
        return new CdaBuilder();
    }


    public static final class CdaBuilder extends Builder {


        private final Map<String,String> atCreationMdc;

        public CdaBuilder() {
            var tmp = MDC.getCopyOfContextMap(); // get copy at time of creation of this context
            atCreationMdc = tmp == null ? new HashMap<>() : tmp;
        }

        /**
         * Takes the MDC values that existed at the time of creation of this new Context
         * and the most recent context then adds the Metadata/Tag values that were set
         * on this context.
         *
         * MDC data is ThreadLocal, so the above behavior is required to allow the context to
         * properly propagate to threads, where are currently used in the {@see RatingMetaDataDao} (though no logging is done there)
         *
         */
        @Override
        public LoggingContextCloseable install() {
            final var meta = this.getMetadata();
            final var currentMdc = MDC.getCopyOfContextMap();

            atCreationMdc.putAll(atCreationMdc);
            MDC.setContextMap(atCreationMdc);

            if (meta != null) {
                for (int i = 0; i < meta.size(); i++) {
                    var key = meta.getKey(i);
                    var val = meta.getValue(i);
                    MDC.put(key.getLabel(), val.toString());
                }
            }

            final var tags = this.getTags();
            if (tags != null) {
                tags.asMap().forEach((key, entry) ->
                    MDC.put(key, String.join(",", entry.stream()
                                                       .map(Object::toString)
                                                       .collect(Collectors.toList())))
                );
            }

            // Just reset back the MDC map to whatever it was before we decided to muck with it.
            return () -> {
                MDC.clear();
                MDC.setContextMap(currentMdc);
            };
        }
    }
}
