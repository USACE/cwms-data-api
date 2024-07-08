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

import cwms.cda.data.dto.County;
import org.jooq.DSLContext;

import java.util.List;

import static usace.cwms.db.jooq.codegen.tables.AV_COUNTY.AV_COUNTY;

public class CountyDao extends JooqDao<County> {
    public CountyDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * Returns all counties in CDA.
     * 
     * @return a list of counties
     * 
     * @see List
     */
    public List<County> getCounties() {
        return dsl.select(AV_COUNTY.COUNTY_NAME, AV_COUNTY.COUNTY_ID, AV_COUNTY.STATE_INITIAL)
                .from(AV_COUNTY)
                .orderBy(AV_COUNTY.STATE_INITIAL.asc(), AV_COUNTY.COUNTY_ID.asc())
                .fetch()
                .map(r -> new County(r.get(AV_COUNTY.COUNTY_NAME), r.get(AV_COUNTY.COUNTY_ID), r.get(AV_COUNTY.STATE_INITIAL)));
    }

}
