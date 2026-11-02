package fixtures.doubles;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;

import io.javalin.config.Key;
import io.javalin.config.MultipartConfig;
import io.javalin.http.Context;
import io.javalin.http.HttpStatus;
import io.javalin.json.JsonMapper;
import io.javalin.plugin.ContextPlugin;
import io.javalin.router.Endpoint;
import io.javalin.router.Endpoints;
import io.javalin.security.RouteRole;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * It is possible this could just be replaced with better use of ContextMock as
 * used in the previously fixed controllers. However, given this isn't super complex and the structure
 * was already different I'm going to leave this in place and come back to it later given this is already a 
 * rather large set of changes.
 */
public class ContextDouble implements Context
{

    final String queryString;
    final Map<String, String> pathParams = new HashMap<>();
    final Map<Key<?>, Object> appData = new HashMap<>();
    final HttpServletRequest request;
    final HttpServletResponse response;

    public ContextDouble(String queryString, Map<String, String> pathParams, Map<Key<?>, Object> appData, HttpServletRequest request, HttpServletResponse response) {
        this.queryString = queryString;
        this.pathParams.putAll(pathParams);
        this.appData.putAll(appData);
        this.request = request;
        this.response = response;
    }

    @Override
    public <T> T appData(Key<T> arg0) {
        return (T)appData.get(arg0);
    }

    @Override
    public Endpoints endpoints() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'endpoints'");
    }

    @Override
    public Endpoint endpoint() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'endpoint'");
    }

    @Override
    public void future(Supplier<? extends CompletableFuture<?>> arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'future'");
    }

    @Override
    public JsonMapper jsonMapper() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'jsonMapper'");
    }

    @Override
    public Context minSizeForCompression(int arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'minSizeForCompression'");
    }

    @Override
    public HttpServletRequest req() {
        return request;
    }

    @Override
    public HttpServletResponse res() {
        return response;
    }

    @Override
    public String queryString() {
        return queryString;
    }

    @Override
    public MultipartConfig multipartConfig() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'multipartConfig'");
    }

    @Override
    public Map<String, String> pathParamMap() {
        return pathParams;
    }

    @Override
    public ServletOutputStream outputStream() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'outputStream'");
    }

    @Override
    public String pathParam(String arg0) {
        return pathParams.get(arg0);
    }

    @Override
    public void redirect(String arg0, HttpStatus arg1) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'redirect'");
    }

    @Override
    public Context result(InputStream arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'result'");
    }

    @Override
    public boolean strictContentTypes() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'strictContentTypes'");
    }

    @Override
    public InputStream resultInputStream() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'resultInputStream'");
    }

    @Override
    public Context skipRemainingHandlers() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'skipRemainingHandlers'");
    }

    @Override
    public Set<RouteRole> routeRoles() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'routeRoles'");
    }

    @Override
    public <T> T with(Class<? extends ContextPlugin<?, T>> arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'with'");
    }

    @Override
    public void writeJsonStream(Stream<?> arg0) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'writeJsonStream'");
    }
    
}
