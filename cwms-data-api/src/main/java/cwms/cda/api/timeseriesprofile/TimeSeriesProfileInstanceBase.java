/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.timeseriesprofile;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileInstanceDao;
import org.jooq.DSLContext;

public class TimeSeriesProfileInstanceBase {
    static final String TAG = "TimeSeries";
    public static final String PARAMETER_ID = "parameter-id";
    public static final String PARAMETER_ID_MASK = "parameter-id-mask";
    public static final String VERSION_MASK = "version-mask";
    public static final String PROFILE_DATA = "profile-data";
    public static final String START_INCLUSIVE = "start-inclusive";
    public static final String END_INCLUSIVE = "end-inclusive";
    public static final String PREVIOUS = "previous";
    public static final String NEXT = "next";
    private MetricRegistry metrics;

    TimeSeriesProfileInstanceDao getContractDao(DSLContext dsl) {
        return new TimeSeriesProfileInstanceDao(dsl);
    }

    Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    void tspMetrics(MetricRegistry metrics) {
        this.metrics = metrics;
    }
}
