package cwms.cda.features;

import org.togglz.core.Feature;
import org.togglz.core.repository.FeatureState;
import org.togglz.core.repository.StateRepository;

public class CdaFeatureStateRepository implements StateRepository
{

    @Override
    public FeatureState getFeatureState(Feature feature) {
        return null;
    }

    @Override
    public void setFeatureState(FeatureState featureState) {
        System.out.println("Good bye");
        System.exit(3);
    }
    
}
