package fixtures;

import io.javalin.http.HttpCode;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Locale;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;



public class TestHttpServletResponse implements HttpServletResponse {

    /**
     * We will default to OK in all handlers as that is the behavior we see from javalin.
     */
    private int status = HttpCode.OK.getStatus();
    private String contentType="<not set by application>";

    private ServletOutputStream output = new TestServletOutputStream();

    @Override
    public void flushBuffer() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getBufferSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getCharacterEncoding() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public Locale getLocale() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        return output;
    }

    @Override
    public PrintWriter getWriter() throws IOException {
        return new PrintWriter(output);
    }

    @Override
    public boolean isCommitted() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void reset() {
        // TODO Auto-generated method stub

    }

    @Override
    public void resetBuffer() {
        // TODO Auto-generated method stub

    }

    @Override
    public void setBufferSize(int arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setCharacterEncoding(String arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setContentLength(int arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setContentLengthLong(long arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setContentType(String arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setLocale(Locale arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addCookie(Cookie arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addDateHeader(String arg0, long arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addHeader(String arg0, String arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addIntHeader(String arg0, int arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean containsHeader(String arg0) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String encodeRedirectURL(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String encodeRedirectUrl(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String encodeURL(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String encodeUrl(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getHeader(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<String> getHeaderNames() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<String> getHeaders(String arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public void sendError(int arg0) throws IOException {
        status = arg0;
    }

    @Override
    public void sendError(int arg0, String arg1) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void sendRedirect(String arg0) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void setDateHeader(String arg0, long arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setHeader(String arg0, String arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setIntHeader(String arg0, int arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setStatus(int arg0) {
        status = arg0;

    }

    @Override
    public void setStatus(int arg0, String arg1) {
        throw new UnsupportedOperationException("Don't use this form");
    }

}
