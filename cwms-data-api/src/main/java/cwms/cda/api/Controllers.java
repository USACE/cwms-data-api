/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.texttimeseries.TimeSeriesTextMode;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.core.validation.Validator;


public final class Controllers {


    public static final String GET_ONE = "getOne";
    public static final String GET_ALL = "getAll";
    public static final String CREATE = "create";
    public static final String UPDATE = "update";
    public static final String DELETE = "delete";
    public static final String CURSOR = "cursor";
    public static final String PAGE = "page";
    public static final String PAGE_SIZE = "page-size";

    // IF the constant has a number at the end its a deprecated variant

    public static final String SIZE = "size";

    public static final String OFFICE = "office";
    public static final String UNIT = "unit";
    public static final String COUNT = "count";
    public static final String TIME = "time";
    public static final String RESULTS = "results";

    public static final String LIKE = "like";

    public static final String UNIT_SYSTEM = "unit-system";

    public static final String TIMESERIES_CATEGORY_LIKE = "timeseries-category-like";

    public static final String LOCATION_CATEGORY_LIKE = "location-category-like";
    public static final String LOCATION_GROUP_LIKE = "location-group-like";




    public static final String TIMESERIES_GROUP_LIKE = "timeseries-group-like";
    public static final String ACCEPT = "Accept";
    public static final String CLOB_ID = "clob-id";
    public static final String INCLUDE_VALUES = "include-values";
    public static final String FAIL_IF_EXISTS = "fail-if-exists";
    public static final String IGNORE_NULLS = "ignore-nulls";
    public static final String EFFECTIVE_DATE = "effective-date";
    public static final String DATE = "date";
    public static final String LEVEL_ID = "level-id";
    public static final String LEVEL_ID_MASK = "level-id-mask";
    public static final String NAME = "name";
    public static final String CASCADE_DELETE = "cascade-delete";
    public static final String DATUM = "datum";
    public static final String BEGIN = "begin";
    public static final String END = "end";
    public static final String TIMEZONE = "timezone";
    public static final String FORMAT = "format";
    public static final String VERSION = "version";
    public static final String AT = "at";
    public static final String METHOD = "method";
    public static final String START = "start";
    public static final String RATING_ID_MASK = "rating-id-mask";
    public static final String RATING_ID = "rating-id";
    public static final String TEMPLATE_ID = "template-id";
    public static final String TEMPLATE_ID_MASK = "template-id-mask";
    public static final String STORE_TEMPLATE = "store-template";

    public static final String TIMESERIES_ID_REGEX = "timeseries-id-regex";
    public static final String TIMESERIES_ID = "timeseries-id";
    public static final String SNAP_FORWARD = "snap-forward";
    public static final String SNAP_BACKWARD = "snap-backward";
    public static final String ACTIVE = "active";
    public static final String INTERVAL_OFFSET = "interval-offset";
    public static final String INTERVAL = "interval";
    public static final String CATEGORY_ID = "category-id";
    public static final String EXAMPLE_DATE = "2021-06-10T13:00:00-0700[PST8PDT]";
    public static final String VERSION_DATE = "version-date";
    public static final String CREATE_AS_LRTS = "create-as-lrts";
    public static final String STORE_RULE = "store-rule";
    public static final String OVERRIDE_PROTECTION = "override-protection";
    public static final String START_TIME_INCLUSIVE = "start-time-inclusive";
    public static final String END_TIME_INCLUSIVE = "end-time-inclusive";
    public static final String MAX_VERSION = "max-version";
    public static final String TIMESERIES = "timeseries";
    public static final String LOCATIONS = "locations";

    public static final String GROUP_ID = "group-id";
    public static final String REPLACE_ASSIGNED_LOCS = "replace-assigned-locs";
    public static final String REPLACE_ASSIGNED_TS = "replace-assigned-ts";
    public static final String TS_IDS = "ts-ids";
    public static final String DATE_FORMAT = "YYYY-MM-dd'T'hh:mm:ss[Z'['VV']']";
    public static final String INCLUDE_ASSIGNED = "include-assigned";
    public static final String ANY_MASK = "*";
    public static final String ID_MASK = "id-mask";
    public static final String NAME_MASK = "name-mask";
    public static final String BOTTOM_MASK = "bottom-mask";
    public static final String TOP_MASK = "top-mask";
    public static final String INCLUDE_EXPLICIT = "include-explicit";
    public static final String INCLUDE_IMPLICIT = "include-implicit";
    public static final String POOL_ID = "pool-id";
    public static final String PROJECT_ID = "project-id";
    public static final String NOT_SUPPORTED_YET = "Not supported yet.";
    public static final String BOUNDING_OFFICE_LIKE = "bounding-office-like";
    public static final String SPECIFIED_LEVEL_ID = "specified-level-id";
    public static final String HAS_DATA = "has-data";
    public static final String STATUS_200 = "200";
    public static final String STATUS_201 = "201";
    public static final String STATUS_404 = "404";
    public static final String STATUS_501 = "501";
    public static final String STATUS_400 = "400";
    public static final String TEXT_MASK = "text-mask";
    public static final String DELETE_MODE = "delete-mode";
    public static final String MIN_ATTRIBUTE = "min-attribute";
    public static final String MAX_ATTRIBUTE = "max-attribute";

    static {
        JavalinValidation.register(JooqDao.DeleteMethod.class, Controllers::getDeleteMethod);
        JavalinValidation.register(TimeSeriesTextMode.class, TimeSeriesTextMode::getMode);
    }

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
        Meter meter = registry.meter(name(className, subject, COUNT));
        meter.mark();
        Timer timer = registry.timer(name(className, subject, TIME));
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

    public static JooqDao.DeleteMethod getDeleteMethod(String input) {
        JooqDao.DeleteMethod retval = null;

        if (input != null) {
            retval = JooqDao.DeleteMethod.valueOf(input.toUpperCase());
        }
        return retval;
    }
}
