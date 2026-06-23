package cwms.cda;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

public final class OpenTelemetrySetup {
    private OpenTelemetrySetup() {
        /* This utility class should not be instantiated */
    }

    /**
   * Initializes the OpenTelemetry SDK with a logging span exporter and the W3C Trace Context
   * propagator.
   *
   * @return A ready-to-use {@link OpenTelemetry} instance.
   */
    static void initTelemetry() {
        SdkTracerProvider sdkTracerProvider =
            SdkTracerProvider.builder()
       //         .addSpanProcessor(SimpleSpanProcessor.create(new LoggingSpanExporter()))
                .build();

        OpenTelemetrySdk sdk =
            OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
        GlobalOpenTelemetry.set(sdk);
        Runtime.getRuntime().addShutdownHook(new Thread(sdkTracerProvider::close));
       
    }

}
