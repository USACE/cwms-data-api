package fixtures;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;

public class TestServletOutputStream extends ServletOutputStream {

    ByteArrayOutputStream realOutput = new ByteArrayOutputStream();

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public void setWriteListener(WriteListener arg0) {

    }

    @Override
    public void write(int b) throws IOException {
        realOutput.write(b);
    }

    public String getOutput(){
        return realOutput.toString();
    }

}
