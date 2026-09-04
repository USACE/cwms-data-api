package cwms.cda.servlet;

import java.io.IOException;
import java.util.Set;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Converts the error dispatch used to load known client-side routes into a successful response.
 */
@WebFilter(urlPatterns = {"/index.html"}, dispatcherTypes = {DispatcherType.ERROR})
public final class SpaErrorStatusFilter implements Filter {

    // Keep these paths synchronized with cda-gui/src/route-paths.js.
    private static final Set<String> SPA_ROUTES = Set.of(
        "/data-query",
        "/filter-expressions",
        "/legacy-format",
        "/location-search",
        "/quick-start",
        "/disclaimer",
        "/site-map",
        "/regexp",
        "/swagger-ui",
        "/timestamps",
        "/user-lists"
    );

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest)request;
        HttpServletResponse httpResponse = (HttpServletResponse)response;

        if (isClientRoute(httpRequest)) {
            httpResponse.setStatus(HttpServletResponse.SC_OK);
        }

        chain.doFilter(request, response);
    }

    private boolean isClientRoute(HttpServletRequest request) {
        String method = request.getMethod();
        if (!"GET".equalsIgnoreCase(method) && !"HEAD".equalsIgnoreCase(method)) {
            return false;
        }

        Object errorRequestUri = request.getAttribute(RequestDispatcher.ERROR_REQUEST_URI);
        if (!(errorRequestUri instanceof String)) {
            return false;
        }

        String path = removeContextPath((String)errorRequestUri, request.getContextPath());
        if (path.length() > 1 && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return SPA_ROUTES.contains(path);
    }

    private String removeContextPath(String requestUri, String contextPath) {
        if (contextPath == null || contextPath.isEmpty()) {
            return requestUri;
        }
        if (requestUri.equals(contextPath)) {
            return "/";
        }
        if (requestUri.startsWith(contextPath + "/")) {
            return requestUri.substring(contextPath.length());
        }
        return requestUri;
    }
}
