/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dao.location.kind;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.LocationGroupDao;
import cwms.cda.data.dto.AssignedLocation;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.location.kind.Outlet;
import java.util.List;
import java.util.Optional;
import org.jooq.DSLContext;

public class BaseOutletDaoIT extends ProjectStructureIT {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();

    public static void deleteLocationGroup(DSLContext context, Outlet outlet) {
        LocationGroupDao locationGroupDao = new LocationGroupDao(context);
        try {
            locationGroupDao.delete(outlet.getRatingCategoryId().getName(), outlet.getRatingGroupId().getName(), true, OFFICE_ID);
        } catch (NotFoundException e) {
            LOGGER.atFinest().withCause(e).log("No data found for category:" + outlet.getRatingCategoryId().getName()
                                                       + ", group-id:" + outlet.getRatingGroupId().getName());
        }
    }

    public static void createRatingSpecForOutlet(DSLContext context, Outlet outlet, String specId) {
        LocationGroupDao locGroupDao = new LocationGroupDao(context);
        Optional<LocationGroup> ratingGroup = locGroupDao.getLocationGroup(
                outlet.getRatingCategoryId().getOfficeId(),
                outlet.getRatingCategoryId().getName(),
                outlet.getRatingGroupId().getName());

        if (ratingGroup.isPresent()) {
            LocationGroup realGroup = ratingGroup.get();
            List<AssignedLocation> assLocs = realGroup.getAssignedLocations();
            realGroup = new LocationGroup(realGroup.getLocationCategory(), realGroup.getOfficeId(),
                                          realGroup.getId(), realGroup.getDescription(),
                                          specId, realGroup.getSharedRefLocationId(),
                                          realGroup.getLocGroupAttribute());
            realGroup = new LocationGroup(realGroup, assLocs);
            locGroupDao.delete(realGroup.getLocationCategory().getId(), realGroup.getId(), true,
                               realGroup.getOfficeId());
            locGroupDao.create(realGroup);
        }
    }
}
