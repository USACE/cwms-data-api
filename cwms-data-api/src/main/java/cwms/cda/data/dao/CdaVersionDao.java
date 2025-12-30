/*
 *
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

package cwms.cda.data.dao;

import cwms.cda.ApiServlet;
import cwms.cda.data.dto.CdaVersion;
import java.util.HashMap;
import java.util.Map;
import com.google.common.flogger.FluentLogger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

public final class CdaVersionDao extends JooqDao<CdaVersion> {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    public CdaVersionDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * Retrieves the CDA version information along with relevant feature attributes.
     *
     * @return CdaVersion object containing version and features
     */
    public CdaVersion getCdaVersion() {
        return new CdaVersion.Builder()
                .withVersion(ApiServlet.getApiVersion())
                .withFeatures(buildFeatures())
                .build();
    }

    private Map<String, Object> buildFeatures() {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("schema", Dao.getVersion(super.dsl));
        attrs.put("ts", buildTsFeatures());
        // Add other feature attributes as needed
        return attrs;
    }

    private String hasTsDataEntryDateSupport() {
        TimeSeriesDaoImpl tsDao = new TimeSeriesDaoImpl(super.dsl);
        boolean supported = false;
        try {
            tsDao.validateEntryDateSupport(true);
            supported = true;
        } catch (DataAccessException e) {
            logger.atFinest().withCause(e).log("%s", e.getMessage());
        }
        return String.valueOf(supported);
    }

    private Map<String, String> buildTsFeatures() {
        Map<String, String> tsAttrs = new HashMap<>();
        tsAttrs.put("data_entry_date_support", hasTsDataEntryDateSupport());
        // Add other time series specific feature attributes as needed
        return tsAttrs;
    }
}
