package cwms.cda;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;

public final class OpenTelemetrySetup {
    private OpenTelemetrySetup() {
        /* This utility class should not be instantiated */
    }

    /**
   * Initializes the OpenTelemetry SDK with a logging span exporter and the W3C Trace Context
   * propagator.
   *
   */
    @SuppressWarnings("null") // nothing here can be null without other exceptions getting thrown.
    public static void initTelemetry() {
        SdkTracerProvider sdkTracerProvider =
            SdkTracerProvider.builder()
                .build();
        
        OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal();
        Runtime.getRuntime().addShutdownHook(new Thread(sdkTracerProvider::close));
       
    }
}
