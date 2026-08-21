package cwms.cda.servlet;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;

@WebListener
public class InitListener implements ServletContextListener {
    
    private static final SdkTracerProvider sdkTracerProvider =
            SdkTracerProvider.builder()
                .build();

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        
        
        OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal();
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        sdkTracerProvider.close();
    }
}
