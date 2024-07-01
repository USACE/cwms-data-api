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

import cwms.cda.data.dto.State;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_STATE;

import java.util.List;

public class StateDao extends JooqDao<State> {
    public StateDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * Returns all states in CDA.
     *
     * @return a list of states
     *
     * @see List
     */
    public List<State> getStates() {
        return CWMS_CAT_PACKAGE.call_CAT_STATE(dsl.configuration())
                .map(r -> new State(r.get(AV_STATE.AV_STATE.STATE_INITIAL), r.get("STATE_NAME", String.class)));
    }

}
