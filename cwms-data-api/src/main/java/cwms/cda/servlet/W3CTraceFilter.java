package cwms.cda.servlet;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;

import cwms.cda.OpenTelemetrySetup;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.propagation.TextMapGetter;

/**
 * 
 */
@WebFilter(urlPatterns = {"*"})
public final class W3CTraceFilter implements Filter {

    public static final ContextKey<String> TRACE_PARENT = ContextKey.named("traceparent");
    public static final Pattern TRACE_PARENT_MATCHER =
        Pattern.compile("[a-z0-9]{2}-[a-z0-9]{32}-[a-z0-9]{16}-[a-z0-9]{2}");

    public W3CTraceFilter() {
        OpenTelemetrySetup.initTelemetry();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        var spanBuilder = GlobalOpenTelemetry.getTracer("cda")
            .spanBuilder("Request")
            .setSpanKind(SpanKind.SERVER);
        var provided = ((HttpServletRequest)request).getHeader(TRACE_PARENT.toString());
        if (provided != null && !provided.isEmpty() && TRACE_PARENT_MATCHER.matcher(provided).matches()) {
            var propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
            var ctx = propagator.extract(Context.current(), provided, new TraceGetter());
            spanBuilder.setParent(ctx);
        }
        
        var span = spanBuilder.startSpan();
        try (var scope = span.makeCurrent()) {
            chain.doFilter(request, response);
        } finally {
            span.end();
        }
    }
    
    /**
     * A simple wrapper to just get the value in the required way.
     */
    private static class TraceGetter implements TextMapGetter<String>
    {
        @Override
        public Iterable<String> keys(@Nonnull String carrier)
        {
            return List.of(TRACE_PARENT.toString());
        }

        @Override
        @Nullable
        public String get(@Nullable String carrier, @Nonnull String key) {
            if (TRACE_PARENT.toString().equalsIgnoreCase(key)) {
                return carrier;
            } else {
                return null;
            }
        }
    }
}
