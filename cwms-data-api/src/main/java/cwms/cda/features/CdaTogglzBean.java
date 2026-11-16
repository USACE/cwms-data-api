package cwms.cda.features;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import javax.sql.DataSource;

import org.togglz.core.Feature;
import org.togglz.core.manager.TogglzConfig;
import org.togglz.core.repository.StateRepository;
import org.togglz.core.spi.BeanFinder;
import org.togglz.core.user.NoOpUserProvider;
import org.togglz.core.user.UserProvider;

public class CdaTogglzBean implements BeanFinder
{

    @Override
    public <E> Collection<E> find(Class<E> clazz, Object context)
    {
        if (context instanceof DataSource ds)
        {
            return Collections.singletonList((E)new TogglzConfig() {

                @Override
                public Class<? extends Feature> getFeatureClass() {
                    return CdaFeatures.class;
                }

                @Override
                public StateRepository getStateRepository() {
                    return new CdaFeatureStateRepository();
                }

                @Override
                public UserProvider getUserProvider() {
                    return new NoOpUserProvider();
                }
                
            });
        }
        return Collections.emptyList();
    }
    
}
