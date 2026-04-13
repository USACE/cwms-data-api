package cwms.cda.logging;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;

import com.google.common.flogger.MetadataKey;
import com.google.common.flogger.context.ScopedLoggingContexts;

@WebFilter("/*")
public class TraceIdFilter implements Filter
{
    public static final String CONTEXT_TRACE_ID = "traceID";
    public static final String HEADER_TRACE_ID = "X-Trace-ID";

    public static final Pattern UUID_MATCHER = Pattern.compile("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}");

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            var httpRequest = (HttpServletRequest)request;

            var xTraceId = httpRequest.getHeader(HEADER_TRACE_ID);

            String traceId = null;
            if (xTraceId == null || xTraceId.isBlank()) {
                traceId = UUID.randomUUID().toString();
            } else {
                traceId = validate(xTraceId); //well that needs some validation.
            }

            request.setAttribute(HEADER_TRACE_ID, traceId);

            ScopedLoggingContexts.newContext()
                                 .withMetadata(MetadataKey.single("traceId", String.class), traceId)
                                 .callUnchecked(() -> {
                                    chain.doFilter(httpRequest, response);
                                    return null;
                                 });
        } else {
            chain.doFilter(request, response);
        }
    }
    
    private static String validate(String id) throws IOException
    {
        if (UUID_MATCHER.matcher(id).matches()) {
            return id;
        } else {
            throw new IOException("Trace id '" + id + "' is not a valid UUIDish value.");
        }
    }
}
