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
import javax.servlet.http.HttpServletResponse;

import com.google.common.flogger.MetadataKey;
import com.google.common.flogger.context.ScopedLoggingContexts;

@WebFilter("/*")
public class TraceIdFilter implements Filter
{
    public static final String CONTEXT_TRACE_ID = "traceID";
    public static final String HEADER_TRACE_ID = "X-Trace-ID";
    public static final String MSG = "Invalid UUID value provided in X-Trace-Id header.";
    public static final String ERROR_MESSAGE = "{\"message\": \"" + MSG + "\"}";

    // accordning to https://www.rfc-editor.org/rfc/rfc4122.html (section 3) UUID string are case insensitive on input.
    public static final Pattern UUID_MATCHER = Pattern.compile("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}", Pattern.CASE_INSENSITIVE);

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
                try {
                traceId = validate(xTraceId); //well that needs some validation.
                } catch (IOException ex) {
                    var httpResponse = (HttpServletResponse)response;
                    httpResponse.setContentType("application/json");
                    httpResponse.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    var writer = httpResponse.getWriter();
                    writer.println(ERROR_MESSAGE);
                    return;
                }
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
