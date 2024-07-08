package fixtures;

import java.io.IOException;
import java.io.Reader;

import freemarker.cache.TemplateLoader;

public class CdaTemplateLoader implements TemplateLoader
{

    @Override
    public Object findTemplateSource(String name) throws IOException
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'findTemplateSource'");
    }

    @Override
    public long getLastModified(Object templateSource) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getLastModified'");
    }

    @Override
    public Reader getReader(Object templateSource, String encoding) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getReader'");
    }

    @Override
    public void closeTemplateSource(Object templateSource) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'closeTemplateSource'");
    }
    
}
