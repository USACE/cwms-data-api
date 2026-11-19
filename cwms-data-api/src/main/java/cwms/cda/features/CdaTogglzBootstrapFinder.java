package cwms.cda.features;

import java.util.Collection;
import java.util.Collections;
import javax.sql.DataSource;

import org.togglz.core.bootstrap.TogglzBootstrap;
import org.togglz.core.spi.BeanFinder;

import com.google.auto.service.AutoService;

@AutoService(BeanFinder.class)
public class CdaTogglzBootstrapFinder implements BeanFinder
{

    @SuppressWarnings("unchecked")
    @Override
    public <E> Collection<E> find(Class<E> clazz, Object context)
    {        
        if (clazz == TogglzBootstrap.class && context instanceof DataSource ds)
        {
            return Collections.singletonList((E)new CdaTogglzBootstrap(ds));
        }
        return Collections.emptyList();
    }
}
