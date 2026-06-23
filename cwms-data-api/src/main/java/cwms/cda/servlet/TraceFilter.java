package cwms.cda.servlet;

import java.io.IOException;
import java.util.List;

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

@WebFilter(urlPatterns = {"*"})
public final class TraceFilter implements Filter {

    private static final ContextKey<String> TRACE_PARENT = ContextKey.named("traceparent");

    public TraceFilter() {
        OpenTelemetrySetup.initTelemetry();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        var spanBuilder = GlobalOpenTelemetry.getTracer("cda")
            .spanBuilder("Request")
            .setSpanKind(SpanKind.SERVER);
        var provided = ((HttpServletRequest)request).getHeader(TRACE_PARENT.toString());
        if (provided != null && !provided.isEmpty()) {
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
    
    private static class TraceGetter implements TextMapGetter<String>
    {

        @Override
        public Iterable<String> keys(String carrier)
        {
            return List.of(TRACE_PARENT.toString());
        }

        @Override
        @Nullable
        public String get(@Nullable String carrier, String key) {
            if (TRACE_PARENT.toString().equalsIgnoreCase(key)) {
                return carrier;
            } else {
                return null;
            }
        }
    }
}
