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
import cwms.cda.data.dto.Office;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.codegen.tables.AV_COUNTY;

import java.util.List;

public class CountyDao extends JooqDao<Office> {
    public CountyDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * Returns all offices in CDA.
     * * @param hasData specifies whether the office has data in CDA or not (all
     * offices)
     * 
     * @return a list of offices
     * 
     * @see List
     */
    public List<County> getCounties() {
        AV_COUNTY view = AV_COUNTY.AV_COUNTY;
        return dsl.select(AV_COUNTY.AV_COUNTY.COUNTY_NAME, AV_COUNTY.AV_COUNTY.COUNTY_ID, AV_COUNTY.AV_COUNTY.STATE_INITIAL)
                .from(view)
                .orderBy(AV_COUNTY.AV_COUNTY.STATE_INITIAL.asc(), AV_COUNTY.AV_COUNTY.COUNTY_ID.asc())
                .fetch()
                .into(County.class);
    }

}
