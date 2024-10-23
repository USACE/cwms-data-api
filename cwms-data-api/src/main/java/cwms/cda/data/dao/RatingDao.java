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

import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;

import java.io.IOException;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface RatingDao {

    Pattern officeMatcher = Pattern.compile(".*office-id=\"(.*?)\"");

    void create(String ratingSet, boolean storeTemplate) throws IOException, RatingException;

    RatingSet retrieve(RatingSet.DatabaseLoadMethod method, String officeId, String specificationId,
                       Instant start, Instant end, Instant effectiveDate) throws IOException, RatingException;

    String retrieveRatings(String format, String names, String unit, String datum, String office,
                           String start, String end, String timezone);


    void store(String ratingSet, boolean storeTemplate) throws IOException, RatingException;

    void delete(String officeId, String specificationId, Instant start, Instant end);

    static String extractOfficeFromXml(String xml) {
        Matcher officeMatch = officeMatcher.matcher(xml);

        if(officeMatch.find()) {
            return officeMatch.group(1);
        } else {
            throw new RuntimeException("Unable to determine office for data set");
        }
    }
}
