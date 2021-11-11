package fixtures;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;

public class TestServletInputStream extends ServletInputStream {


    private InputStream realStream = null;

    public TestServletInputStream(String data) {
        this(new ByteArrayInputStream(data.getBytes()));
    }

    public TestServletInputStream(InputStream stream) {
        realStream = stream;
    }

    @Override
    public boolean isFinished() {
        // TODO Auto-generated method stub
        try {
            return realStream.available() == 0;
        } catch (IOException e) {
            return true;
        }
    }

    @Override
    public boolean isReady() {
        // TODO Auto-generated method stub
        try {
            return realStream.available() > 0;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void setReadListener(ReadListener arg0) {

    }

    @Override
    public int read() throws IOException {
        return realStream.read();
    }

}
