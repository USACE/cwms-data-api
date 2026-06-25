package cwms.cda.ui;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class SpaForwardFilter implements Filter {

    private static final String INDEX_PATH = "/index.html";
    private ServletContext servletContext;

    @Override
    public void init(FilterConfig filterConfig) {
        servletContext = filterConfig.getServletContext();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        String path = getRequestPath(httpRequest);

        if (shouldForwardToIndex(httpRequest, path)) {
            RequestDispatcher dispatcher = httpRequest.getRequestDispatcher(INDEX_PATH);
            dispatcher.forward(httpRequest, httpResponse);
            return;
        }

        chain.doFilter(request, response);
    }

    private boolean shouldForwardToIndex(HttpServletRequest request, String path) throws IOException {
        if (!isPageRequest(request) || path == null || path.isEmpty() || "/".equals(path)) {
            return false;
        }

        if (isFileRequest(path) || resourceExists(path)) {
            return false;
        }

        return true;
    }

    private boolean isPageRequest(HttpServletRequest request) {
        String method = request.getMethod();
        return "GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method);
    }

    private boolean isFileRequest(String path) {
        int lastSlash = path.lastIndexOf('/');
        int lastDot = path.lastIndexOf('.');
        return lastDot > lastSlash;
    }

    private boolean resourceExists(String path) throws IOException {
        return servletContext.getResource(path) != null;
    }

    private String getRequestPath(HttpServletRequest request) {
        String uri = request.getRequestURI();
        String contextPath = request.getContextPath();
        if (contextPath != null && !contextPath.isEmpty() && uri.startsWith(contextPath)) {
            return uri.substring(contextPath.length());
        }
        return uri;
    }
}
