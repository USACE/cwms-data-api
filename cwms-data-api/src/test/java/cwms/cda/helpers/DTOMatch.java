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

package cwms.cda.helpers;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.Embankment;
import cwms.cda.data.dto.location.kind.PhysicalStructureChange;
import cwms.cda.data.dto.location.kind.Setting;
import cwms.cda.data.dto.location.kind.Turbine;
import cwms.cda.data.dto.location.kind.TurbineSetting;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.data.dto.stream.StreamNode;
import cwms.cda.data.dto.stream.StreamReach;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

public final class DTOMatch {

    private DTOMatch() {
        throw new AssertionError("Utility class");
    }

    public static void assertMatch(CwmsId first, CwmsId second, String variableName) {
        assertAll(
            () -> Assertions.assertEquals(first.getOfficeId(), second.getOfficeId(),
                variableName + " is not the same. Office ID differs"),
            () -> Assertions.assertEquals(first.getName(), second.getName(),
                variableName + " is not the same. Name differs")
        );
    }

    public static void assertMatch(CwmsId first, CwmsId second) {
        assertMatch(first, second, "LocationIdentifier");
    }


    public static void assertMatch(StreamLocation streamLocation, StreamLocation deserialized) {
        assertAll(
            () -> assertMatch(streamLocation.getId(), deserialized.getId()),
            () -> assertMatch(streamLocation.getStreamNode(), deserialized.getStreamNode()),
            () -> assertEquals(streamLocation.getPublishedStation(), deserialized.getPublishedStation(),
                "The published station does not match"),
            () -> assertEquals(streamLocation.getNavigationStation(), deserialized.getNavigationStation(),
                "The navigation station does not match"),
            () -> assertEquals(streamLocation.getLowestMeasurableStage(), deserialized.getLowestMeasurableStage(),
                "The lowest measurable stage does not match"),
            () -> assertEquals(streamLocation.getTotalDrainageArea(), deserialized.getTotalDrainageArea(),
                "The total drainage area does not match"),
            () -> assertEquals(streamLocation.getUngagedDrainageArea(), deserialized.getUngagedDrainageArea(),
                "The ungaged drainage area does not match"),
            () -> assertEquals(streamLocation.getAreaUnits(), deserialized.getAreaUnits(),
                "The area unit does not match"),
            () -> assertEquals(streamLocation.getStageUnits(), deserialized.getStageUnits(),
                "The stage unit does not match")
        );
    }

    public static void assertMatch(StreamNode node1, StreamNode node2) {
        assertAll(
            () -> assertMatch(node1.getStreamId(), node2.getStreamId()),
            () -> assertEquals(node1.getBank(), node2.getBank()),
            () -> assertEquals(node1.getStation(), node2.getStation()),
            () -> assertEquals(node1.getStationUnits(), node2.getStationUnits())
        );
    }

    public static void assertMatch(StreamReach reach1, StreamReach reach2) {
        assertAll(
            () -> assertEquals(reach1.getComment(), reach2.getComment()),
            () -> assertMatch(reach1.getDownstreamNode(), reach2.getDownstreamNode()),
            () -> assertMatch(reach1.getUpstreamNode(), reach2.getUpstreamNode()),
            () -> assertMatch(reach1.getConfigurationId(), reach2.getConfigurationId()),
            () -> assertMatch(reach1.getStreamId(), reach2.getStreamId()),
            () -> assertMatch(reach1.getId(), reach2.getId())
        );
    }

    public static void assertMatch(Stream stream1, Stream stream2) {
        assertAll(
            () -> assertEquals(stream1.getStartsDownstream(), stream2.getStartsDownstream()),
            () -> assertMatch(stream1.getFlowsIntoStreamNode(), stream2.getFlowsIntoStreamNode()),
            () -> assertMatch(stream1.getDivertsFromStreamNode(), stream2.getDivertsFromStreamNode()),
            () -> assertEquals(stream1.getLength(), stream2.getLength()),
            () -> assertEquals(stream1.getAverageSlope(), stream2.getAverageSlope()),
            () -> assertEquals(stream1.getLengthUnits(), stream2.getLengthUnits()),
            () -> assertEquals(stream1.getSlopeUnits(), stream2.getSlopeUnits()),
            () -> assertEquals(stream1.getComment(), stream2.getComment()),
            () -> assertMatch(stream1.getId(), stream2.getId())
        );
    }

    public static void assertMatch(Embankment first, Embankment second) {

        assertAll(
            () -> assertEquals(first.getUpstreamSideSlope(), second.getUpstreamSideSlope(),
                "Upstream side slope doesn't match"),
            () -> assertEquals(first.getDownstreamSideSlope(), second.getDownstreamSideSlope(),
                "Downstream side slope doesn't match"),
            () -> assertEquals(first.getStructureLength(), second.getStructureLength(),
                "Structure length doesn't match"),
            () -> assertEquals(first.getMaxHeight(), second.getMaxHeight(), "Maximum height doesn't match"),
            () -> assertEquals(first.getTopWidth(), second.getTopWidth(), "Top width doesn't match"),
            () -> assertEquals(first.getLengthUnits(), second.getLengthUnits(), "Units ID doesn't match"),
            () -> assertMatch(first.getDownstreamProtectionType(), second.getDownstreamProtectionType()),
            () -> assertMatch(first.getUpstreamProtectionType(), second.getUpstreamProtectionType()),
            () -> assertMatch(first.getStructureType(), second.getStructureType()),
            () -> assertEquals(first.getLocation(), second.getLocation(), "Location doesn't match"),
            () -> assertMatch(first.getProjectId(), second.getProjectId())
        );
    }

    public static void assertMatch(LookupType lookupType, LookupType deserialized) {
        assertEquals(lookupType.getOfficeId(), deserialized.getOfficeId());
        assertEquals(lookupType.getDisplayValue(), deserialized.getDisplayValue());
        assertEquals(lookupType.getTooltip(), deserialized.getTooltip());
        assertEquals(lookupType.getActive(), deserialized.getActive());
    }

    public static void assertMatch(Turbine first, Turbine second) {
        assertAll(
            () -> assertMatch(first.getProjectId(), second.getProjectId()),
            () -> assertEquals(first.getLocation(), second.getLocation(), "Locations are not the same")
        );
    }

    public static void assertMatch(PhysicalStructureChange first, PhysicalStructureChange second) {
        assertAll(() -> assertMatch(first.getProjectId(), second.getProjectId()),
            () -> assertMatch(first.getReasonType(), second.getReasonType()),
            () -> assertMatch(first.getDischargeComputationType(), second.getDischargeComputationType()),
            () -> assertSettingsMatch(first.getSettings(), second.getSettings()),
            () -> assertEquals(first.getChangeDate(), second.getChangeDate()),
            () -> assertEquals(first.getDischargeUnits(), second.getDischargeUnits()),
            () -> assertEquals(first.getNewTotalDischargeOverride(), second.getNewTotalDischargeOverride()),
            () -> assertEquals(first.getOldTotalDischargeOverride(), second.getOldTotalDischargeOverride()),
            () -> assertEquals(first.getElevationUnits(), second.getElevationUnits()),
            () -> assertEquals(first.getTailwaterElevation(), second.getTailwaterElevation()),
            () -> assertEquals(first.getPoolElevation(), second.getPoolElevation()),
            () -> assertEquals(first.getNotes(), second.getNotes()),
            () -> assertEquals(first.isProtected(), second.isProtected()));
    }

    public static void assertSettingsMatch(Set<Setting> first, Set<Setting> second) {
        List<Executable> assertions = new ArrayList<>();
        assertions.add(() -> assertEquals(first.size(), second.size()));
        for(Setting setting : first) {
            assertions.add(() -> {
                Setting match = second.stream()
                    .filter(
                        s -> s.getLocationId().getOfficeId().equalsIgnoreCase(setting.getLocationId().getOfficeId()))
                    .filter(s -> s.getLocationId().getName().equalsIgnoreCase(setting.getLocationId().getName()))
                    .findAny()
                    .orElseThrow(
                        () -> fail("Setting " + setting.getLocationId().getName() + " not found"));
                assertMatch(setting, match);
            });
        }
        assertAll(assertions.toArray(new Executable[0]));
    }

    public static void assertMatch(Setting first, Setting second) {
        assertAll(() -> assertMatch(first.getLocationId(), second.getLocationId()),
            () -> assertEquals(first.getClass(), second.getClass()),
            () -> {
                if(first instanceof TurbineSetting && second instanceof TurbineSetting) {
                    assertMatch((TurbineSetting) first, (TurbineSetting) second);
                } else {
                    fail("Assertion check for setting type: " + first.getClass() + " not yet supported");
                }
            });
    }

    public static void assertMatch(TurbineSetting first, TurbineSetting second) {
        assertAll(() -> assertEquals(first.getDischargeUnits(), second.getDischargeUnits()),
            () -> assertEquals(first.getNewDischarge(), second.getNewDischarge()),
            () -> assertEquals(first.getOldDischarge(), second.getOldDischarge()),
            () -> assertEquals(first.getGenerationUnits(), second.getGenerationUnits()),
            () -> assertEquals(first.getRealPower(), second.getRealPower()),
            () -> assertEquals(first.getScheduledLoad(), second.getScheduledLoad()));
    }
}
