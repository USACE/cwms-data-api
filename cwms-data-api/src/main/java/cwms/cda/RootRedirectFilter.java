package cwms.cda;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class RootRedirectFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) {
        // No initialization required.
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        if (isContextRootRequest(httpRequest)) {
            String target = httpRequest.getContextPath() + "-ui/";
            httpResponse.sendRedirect(target);
            return;
        }

        chain.doFilter(request, response);
    }

    private boolean isContextRootRequest(HttpServletRequest request) {
        String method = request.getMethod();
        if (!"GET".equalsIgnoreCase(method) && !"HEAD".equalsIgnoreCase(method)) {
            return false;
        }

        String contextPath = request.getContextPath();
        String requestUri = request.getRequestURI();
        return requestUri.equals(contextPath) || requestUri.equals(contextPath + "/");
    }
}
