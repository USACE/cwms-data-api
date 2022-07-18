package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.javalin.core.validation.Validator;


public class Controllers {

    private Controllers() {

    }

    /**
     * Marks a meter and starts a timer.
     *
     * @param registry  Metric Registry
     * @param className Added to the metric names
     * @param subject   Added to the metric names
     * @return Timer.Context of the started timer.
     */
    public static Timer.Context markAndTime(MetricRegistry registry, String className,
                                            String subject) {
        Meter meter = registry.meter(name(className, subject, "count"));
        meter.mark();
        Timer timer = registry.timer(name(className, subject, "time"));
        return timer.time();
    }

    /**
     * Returns the first matching query param or the provided default value if no match is found.
     *
     * @param ctx          Request Context
     * @param names        An ordered list of allowed query parameter names.  Useful for supporting
     *                     deprecated or renamed parameters.  The correct name should be
     *                     specified first
     *                     followed by any number of deprecated names.
     * @param clazz        Return value type.
     * @param defaultValue Value to return if no matching queryParam is found.
     * @return value
     */
    public static <T> T queryParamAsClass(io.javalin.http.Context ctx, String[] names,
                                          Class<T> clazz, T defaultValue) {
        T retval = defaultValue;

        Validator<T> validator = ctx.queryParamAsClass(names[0], clazz);
        if (validator.hasValue()) {
            retval = validator.get();
        } else {
            for (int i = 1; i < names.length; i++) {
                validator = ctx.queryParamAsClass(names[i], clazz);
                if (validator.hasValue()) {
                    retval = validator.get();
                    break;
                }
            }

        }

        return retval;
    }

    /**
     * Returns the first matching query param or the provided default value if no match is found.
     * Records in a metrics counter whether the match was for the first name, one of the deprecated
     * names or the default value.
     *
     * @param ctx          Request Context
     * @param names        An ordered list of allowed query parameter names.  Useful for supporting
     *                     deprecated or renamed parameters.  The correct name should be
     *                     specified first
     *                     followed by any number of deprecated names.
     * @param clazz        Return value type.
     * @param defaultValue Value to return if no matching queryParam is found.
     * @param metrics      Metrics registry
     * @param className    subject for the metrics
     * @return value
     */
    public static <T> T queryParamAsClass(io.javalin.http.Context ctx, String[] names,
                                          Class<T> clazz, T defaultValue, MetricRegistry metrics,
                                          String className) {
        T retval = null;

        Validator<T> validator = ctx.queryParamAsClass(names[0], clazz);
        if (validator.hasValue()) {
            retval = validator.get();
            metrics.counter(name(className, "correct")).inc();
        } else {
            for (int i = 1; i < names.length; i++) {
                validator = ctx.queryParamAsClass(names[i], clazz);
                if (validator.hasValue()) {
                    retval = validator.get();
                    metrics.counter(name(className, "deprecated")).inc();
                    break;
                }
            }

            if (retval == null) {
                retval = defaultValue;
                metrics.counter(name(className, "default")).inc();
            }

        }

        return retval;
    }

}
