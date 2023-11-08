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

package cwms.cda.data.dao;

import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.RecentValue;
import cwms.cda.data.dto.TimeSeries;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public interface TimeSeriesDao {

    Timestamp NON_VERSIONED = null;

    Catalog getTimeSeriesCatalog(String cursor, int pageSize, String office);

    Catalog getTimeSeriesCatalog(String cursor, int pageSize, String office, String idLike,
                                 String locCategoryLike, String locGroupLike,
                                 String tsCategoryLike, String tsGroupLike, String boundingOfficeLike);

    void create(TimeSeries input);

    void create(TimeSeries input,
                Timestamp versionDate,
                boolean createAsLrts, StoreRule replaceAll, boolean overrideProtection);

    void store(TimeSeries timeSeries, Timestamp versionDate);
    void store(TimeSeries timeSeries, Timestamp versionDate, boolean createAsLrts,
               StoreRule replaceAll, boolean overrideProtection);

    void delete(String officeId, String tsId, TimeSeriesDeleteOptions options);

    TimeSeries getTimeseries(String cursor, int pageSize, String names, String office,
                             String unit, String datum, ZonedDateTime begin, ZonedDateTime end,
                             ZoneId timezone);

    String getTimeseries(String format, String names, String office, String unit, String datum,
                         ZonedDateTime begin, ZonedDateTime end, ZoneId timezone);


    List<RecentValue> findRecentsInRange(String office, String categoryId, String groupId,
                                         Timestamp pastLimit, Timestamp futureLimit);

    List<RecentValue> findMostRecentsInRange(List<String> tsIds, Timestamp pastLimit, Timestamp futureLimit);

}
