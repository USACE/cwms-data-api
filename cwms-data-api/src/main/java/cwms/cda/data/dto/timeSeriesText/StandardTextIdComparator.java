/*
 * Copyright (c) 2021. Hydrologic Engineering Center (HEC).
 * United States Army Corps of Engineers
 * All Rights Reserved. HEC PROPRIETARY/CONFIDENTIAL.
 * Source may not be released without written approval from HEC
 *
 */

package cwms.cda.data.dto.timeSeriesText;

import java.util.Comparator;

public class StandardTextIdComparator implements Comparator<StandardTextId> {
    public StandardTextIdComparator() {

    }

    @Override
    public int compare(StandardTextId o1, StandardTextId o2) {
        String standardTextId1 = null;
        String standardTextId2 = null;
        String officeId1 = null;
        String officeId2 = null;

        if (o1 != null) {
            standardTextId1 = o1.getId();
            officeId1 = o1.getOfficeId();
        }
        if (o2 != null) {
            standardTextId2 = o2.getId();
            officeId2 = o2.getOfficeId();
        }

        // compare officeId first and then standardTextId
        int compare = compareStrings(officeId1, officeId2);
        if (compare == 0) {
            compare = compareStrings(standardTextId1, standardTextId2);
        }
        return compare;
    }

    private int compareStrings(String s1, String s2) {
        if (s1 == null && s2 == null) {
            return 0;
        }
        if (s1 == null) {
            return -1;
        }
        if (s2 == null) {
            return 1;
        }
        int compare = String.CASE_INSENSITIVE_ORDER.compare(s1, s2);
        return compare;
    }

}
