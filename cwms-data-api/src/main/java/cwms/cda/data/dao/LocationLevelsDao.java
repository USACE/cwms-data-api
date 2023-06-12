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

import cwms.cda.data.dto.LocationLevel;
import cwms.cda.data.dto.LocationLevels;
import cwms.cda.data.dto.TimeSeries;
import hec.data.level.ILocationLevelRef;
import mil.army.usace.hec.metadata.Interval;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public interface LocationLevelsDao {
    void deleteLocationLevel(String locationLevelName, ZonedDateTime date, String officeId,
                             Boolean cascadeDelete);

    void storeLocationLevel(LocationLevel level, ZoneId zoneId);

    void renameLocationLevel(String oldLocationLevelName, String newLocationLevelName, String officeId);

    LocationLevel retrieveLocationLevel(String locationLevelName, String unitSystem,
                                        ZonedDateTime effectiveDate, String officeId);

    String getLocationLevels(String format, String names, String office, String unit,
                             String datum, String begin,
                             String end, String timezone);

    LocationLevels getLocationLevels(String cursor, int pageSize,
                                     String names, String office, String unit, String datum,
                                     ZonedDateTime beginZdt, ZonedDateTime endZdt);

    TimeSeries retrieveLocationLevelAsTimeSeries(ILocationLevelRef levelRef, Instant start, Instant end, Interval interval);
}
