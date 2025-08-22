/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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

import cwms.cda.data.dto.Location;
import fixtures.CwmsDataApiSetupCallback;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;

public class LocationCleanup implements TestExecutionListener {

    /**
     * List of locations that will need to be deleted when tests are done.
     */
    public static final Set<Location> locationsCreated = new LinkedHashSet<>();

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        try {
            cleanupLocations();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Runs cascade delete on each location. Location not existing is not an error.
     * All other errors will throw a runtime exception and may require manual database
     * cleanup.
     */
    private static void cleanupLocations() throws IOException {

        String deleteLocationQuery = IOUtils.toString(
            CwmsDataApiSetupCallback.class
                .getClassLoader()
                .getResourceAsStream("cwms/cda/data/sql_templates/delete_location.sql"),"UTF-8"
        );
        Iterator<Location> it = locationsCreated.iterator();
        while(it.hasNext()) {
            try {
                Location location = it.next();
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                db.connection((c)-> {
                    try(PreparedStatement stmt = c.prepareStatement(deleteLocationQuery)) {
                        stmt.setString(1,location.getName());
                        stmt.setString(2,location.getOfficeId());
                        stmt.execute();
                    } catch (SQLException ex) {
                        if (ex.getErrorCode() != 20025 /*does not exist*/) {
                            throw new RuntimeException("Unable to remove location",ex);
                        }
                    }
                },"cwms_20");
                it.remove();
            } catch(SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
