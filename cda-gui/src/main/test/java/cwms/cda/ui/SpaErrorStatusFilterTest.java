package cwms.cda.ui;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.FilterChain;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SpaErrorStatusFilterTest {

    private final SpaErrorStatusFilter filter = new SpaErrorStatusFilter();

    @Test
    void registersForIndexErrorDispatches() {
        WebFilter annotation = SpaErrorStatusFilter.class.getAnnotation(WebFilter.class);

        assertArrayEquals(new String[] {"/index.html"}, annotation.urlPatterns());
        assertArrayEquals(new DispatcherType[] {DispatcherType.ERROR}, annotation.dispatcherTypes());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "/api-keys",
        "/api-keys/",
        "/api-keys/help",
        "/api-keys/help/",
        "/data-query",
        "/filter-expressions",
        "/legacy-format",
        "/location-search",
        "/regexp",
        "/swagger-ui",
        "/swagger-ui/",
        "/timestamps",
        "/user-lists",
        "/user-roles",
        "/user-roles/swt",
        "/user-roles/HQ",
        "/user-roles/mvsc/"
    })
    void returnsOkForClientRoutes(String route) throws ServletException, IOException {
        HttpServletRequest request = buildRequest("GET", "/cwms-data" + route);
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        verify(chain).doFilter(request, response);
    }

    @Test
    void returnsOkForHeadRequest() throws ServletException, IOException {
        HttpServletRequest request = buildRequest("HEAD", "/cwms-data/swagger-ui");
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        verify(chain).doFilter(request, response);
    }

    @Test
    void returnsOkForAlternateContextPath() throws ServletException, IOException {
        HttpServletRequest request = buildRequest("GET", "/spk-data/swagger-ui", "/spk-data");
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        verify(chain).doFilter(request, response);
    }

    @Test
    void preservesNotFoundStatusForUnknownRoutes() throws ServletException, IOException {
        HttpServletRequest request = buildRequest("GET", "/cwms-data/not-a-client-route");
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        verify(response, never()).setStatus(HttpServletResponse.SC_OK);
        verify(chain).doFilter(request, response);
    }

    @Test
    void preservesNotFoundStatusForNonPageRequests() throws ServletException, IOException {
        HttpServletRequest request = buildRequest("POST", "/cwms-data/swagger-ui");
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        verify(response, never()).setStatus(HttpServletResponse.SC_OK);
        verify(chain).doFilter(request, response);
    }

    private HttpServletRequest buildRequest(String method, String requestUri) {
        return buildRequest(method, requestUri, "/cwms-data");
    }

    private HttpServletRequest buildRequest(String method, String requestUri, String contextPath) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getMethod()).thenReturn(method);
        when(request.getContextPath()).thenReturn(contextPath);
        when(request.getAttribute(RequestDispatcher.ERROR_REQUEST_URI)).thenReturn(requestUri);
        return request;
    }
}
