package cwms.cda.logging;

import com.google.auto.service.AutoService;
import com.google.common.flogger.context.ContextDataProvider;
import com.google.common.flogger.context.ScopedLoggingContext;

/**
 * Incredible simple implementation of Flogger's ContextDataProvider so that
 * we can manipulate the 
 */
@AutoService(ContextDataProvider.class)
public class CdaContextDataProvider extends ContextDataProvider {

    private static final ScopedLoggingContext CONTEXT = new CdaLoggingContext();

    @Override
    public ScopedLoggingContext getContextApiSingleton() {
        return CONTEXT;
    }
}
