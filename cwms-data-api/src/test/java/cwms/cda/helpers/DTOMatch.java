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

import cwms.cda.data.dto.stream.StreamLocationNode;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.CompoundOutletRecord;
import cwms.cda.data.dto.location.kind.Embankment;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.data.dto.location.kind.PhysicalStructureChange;
import cwms.cda.data.dto.location.kind.Turbine;
import cwms.cda.data.dto.location.kind.TurbineChange;
import cwms.cda.data.dto.location.kind.TurbineSetting;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.data.dto.stream.StreamNode;
import cwms.cda.data.dto.stream.StreamReach;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

@SuppressWarnings({"LongLine", "checkstyle:LineLength"})
public final class DTOMatch {

    private DTOMatch() {
        throw new AssertionError("Utility class");
    }

    public static void assertMatch(CwmsId first, CwmsId second, String variableName) {
        assertAll(
            () -> Assertions.assertEquals(first.getOfficeId(), second.getOfficeId(),variableName + " is not the same. Office ID differs"),
            () -> Assertions.assertEquals(first.getName(), second.getName(),variableName + " is not the same. Name differs")
        );
    }

    public static void assertMatch(CwmsId first, CwmsId second) {
        assertMatch(first, second, "LocationIdentifier");
    }


    public static void assertMatch(StreamLocation streamLocation, StreamLocation deserialized) {
        assertAll(
            () -> assertMatch(streamLocation.getStreamLocationNode(), deserialized.getStreamLocationNode()),
            () -> assertEquals(streamLocation.getPublishedStation(), deserialized.getPublishedStation(), "The published station does not match"),
            () -> assertEquals(streamLocation.getNavigationStation(), deserialized.getNavigationStation(), "The navigation station does not match"),
            () -> assertEquals(streamLocation.getLowestMeasurableStage(), deserialized.getLowestMeasurableStage(), "The lowest measurable stage does not match"),
            () -> assertEquals(streamLocation.getTotalDrainageArea(), deserialized.getTotalDrainageArea(), "The total drainage area does not match"),
            () -> assertEquals(streamLocation.getUngagedDrainageArea(), deserialized.getUngagedDrainageArea(), "The ungaged drainage area does not match"),
            () -> assertEquals(streamLocation.getAreaUnits(), deserialized.getAreaUnits(), "The area unit does not match"),
            () -> assertEquals(streamLocation.getStageUnits(), deserialized.getStageUnits(), "The stage unit does not match")
        );
    }

    public static void assertMatch(StreamLocationNode streamLocationNode, StreamLocationNode streamLocationNode1) {
        assertAll(
            () -> assertMatch(streamLocationNode.getId(), streamLocationNode1.getId()),
            () -> assertMatch(streamLocationNode.getStreamNode(), streamLocationNode1.getStreamNode())
        );
    }

    public static void assertMatch(StreamNode node1, StreamNode node2) {
        assertAll(
            () -> assertMatch(node1.getStreamId(), node2.getStreamId(), "Stream ID does not match"),
            () -> assertEquals(node1.getBank(), node2.getBank(), "Bank does not match"),
            () -> assertEquals(node1.getStation(), node2.getStation(), "Station does not match"),
            () -> assertEquals(node1.getStationUnits(), node2.getStationUnits(), "Station Units do not match")
        );
    }

    public static void assertMatch(StreamReach reach1, StreamReach reach2) {
        assertAll(
            () -> assertEquals(reach1.getComment(), reach2.getComment(), "Comments do not match"),
            () -> assertMatch(reach1.getDownstreamNode(), reach2.getDownstreamNode()),
            () -> assertMatch(reach1.getUpstreamNode(), reach2.getUpstreamNode()),
            () -> assertMatch(reach1.getConfigurationId(), reach2.getConfigurationId(),"Configuration ID does not match"),
            () -> assertMatch(reach1.getStreamId(), reach2.getStreamId(), "Stream ID does not match"),
            () -> assertMatch(reach1.getId(), reach2.getId(), "Stream reach ID does not match")
        );
    }

    public static void assertMatch(Stream stream1, Stream stream2) {
        assertAll(
            () -> assertEquals(stream1.getStartsDownstream(), stream2.getStartsDownstream(),"Starts downstream property does not match"),
            () -> assertMatch(stream1.getFlowsIntoStreamNode(), stream2.getFlowsIntoStreamNode()),
            () -> assertMatch(stream1.getDivertsFromStreamNode(), stream2.getDivertsFromStreamNode()),
            () -> assertEquals(stream1.getLength(), stream2.getLength(), "Length does not match"),
            () -> assertEquals(stream1.getAverageSlope(), stream2.getAverageSlope(), "Average slope does not match"),
            () -> assertEquals(stream1.getLengthUnits(), stream2.getLengthUnits(), "Length units do not match"),
            () -> assertEquals(stream1.getSlopeUnits(), stream2.getSlopeUnits(), "Slope units do not match"),
            () -> assertEquals(stream1.getComment(), stream2.getComment(), "Comment does not match"),
            () -> assertMatch(stream1.getId(), stream2.getId(), "Stream ID does not match")
        );
    }

    public static void assertMatch(Embankment first, Embankment second) {

        assertAll(
            () -> assertEquals(first.getUpstreamSideSlope(), second.getUpstreamSideSlope(),"Upstream side slope doesn't match"),
            () -> assertEquals(first.getDownstreamSideSlope(), second.getDownstreamSideSlope(),"Downstream side slope doesn't match"),
            () -> assertEquals(first.getStructureLength(), second.getStructureLength(),"Structure length doesn't match"),
            () -> assertEquals(first.getMaxHeight(), second.getMaxHeight(), "Maximum height doesn't match"),
            () -> assertEquals(first.getTopWidth(), second.getTopWidth(), "Top width doesn't match"),
            () -> assertEquals(first.getLengthUnits(), second.getLengthUnits(), "Units ID doesn't match"),
            () -> assertMatch(first.getDownstreamProtectionType(), second.getDownstreamProtectionType()),
            () -> assertMatch(first.getUpstreamProtectionType(), second.getUpstreamProtectionType()),
            () -> assertMatch(first.getStructureType(), second.getStructureType()),
            () -> assertEquals(first.getLocation(), second.getLocation(), "Location doesn't match"),
            () -> assertMatch(first.getProjectId(), second.getProjectId(), "Project ID does not match")
        );
    }

    public static void assertMatch(LookupType lookupType, LookupType deserialized) {
        assertEquals(lookupType.getOfficeId(), deserialized.getOfficeId(), "Office IDs do not match");
        assertEquals(lookupType.getDisplayValue(), deserialized.getDisplayValue(), "Display values do not match");
        assertEquals(lookupType.getTooltip(), deserialized.getTooltip(), "Tool tips do not match");
        assertEquals(lookupType.getActive(), deserialized.getActive(), "Active status does not match");
    }

    public static void assertMatch(Turbine first, Turbine second) {
        assertAll(
            () -> assertMatch(first.getProjectId(), second.getProjectId(), "Project IDs do not match"),
            () -> assertEquals(first.getLocation(), second.getLocation(), "Locations are not the same")
        );
    }

    public static void assertMatch(TurbineChange first, TurbineChange second) {
        assertAll(() -> assertMatch(first.getProjectId(), second.getProjectId()),
            () -> assertMatch(first.getReasonType(), second.getReasonType()),
            () -> assertMatch(first.getDischargeComputationType(), second.getDischargeComputationType()),
            () -> assertSettingsMatch(first.getSettings(), second.getSettings()),
            () -> assertEquals(first.getChangeDate(), second.getChangeDate(), "Change dates do not match"),
            () -> assertEquals(first.getDischargeUnits(), second.getDischargeUnits(), "Discharge units do not match"),
            () -> assertEquals(first.getNewTotalDischargeOverride(), second.getNewTotalDischargeOverride(),"New total discharge override does not match"),
            () -> assertEquals(first.getOldTotalDischargeOverride(), second.getOldTotalDischargeOverride(),"Old total discharge override does not match"),
            () -> assertEquals(first.getElevationUnits(), second.getElevationUnits(), "Elevation units do not match"),
            () -> assertEquals(first.getTailwaterElevation(), second.getTailwaterElevation(),"Tailwater elevations do not match"),
            () -> assertEquals(first.getPoolElevation(), second.getPoolElevation(), "Pool elevations do not match"),
            () -> assertEquals(first.getNotes(), second.getNotes(), "Notes do not match"),
            () -> assertEquals(first.isProtected(), second.isProtected(), "Protected status does not match"));
    }

    public static void assertSettingsMatch(Set<TurbineSetting> first, Set<TurbineSetting> second) {
        List<Executable> assertions = new ArrayList<>();
        assertions.add(() -> assertEquals(first.size(), second.size(), "Turbine settings lists sizes do not match"));
        for (TurbineSetting setting : first) {
            assertions.add(() -> {
                TurbineSetting match = second.stream()
                    .filter(
                        s -> s.getLocationId().getOfficeId().equalsIgnoreCase(setting.getLocationId().getOfficeId()))
                    .filter(s -> s.getLocationId().getName().equalsIgnoreCase(setting.getLocationId().getName()))
                    .findAny()
                    .orElseThrow(() -> fail("Setting " + setting.getLocationId().getName() + " not found"));
                assertMatch(setting, match);
            });
        }
        assertAll(assertions.toArray(new Executable[0]));
    }

    public static void assertMatch(TurbineSetting first, TurbineSetting second) {
        assertAll(
            () -> assertEquals(first.getDischargeUnits(), second.getDischargeUnits(), "Discharge units do not match"),
            () -> assertEquals(first.getNewDischarge(), second.getNewDischarge(), "New discharge does not match"),
            () -> assertEquals(first.getOldDischarge(), second.getOldDischarge(), "Old discharge does not match"),
            () -> assertEquals(first.getGenerationUnits(), second.getGenerationUnits(),"Generation units do not match"),
            () -> assertEquals(first.getRealPower(), second.getRealPower(), "Real power does not match"),
            () -> assertEquals(first.getScheduledLoad(), second.getScheduledLoad(), "Scheduled load does not match"));
    }

    public static void assertMatch(Outlet first, Outlet second) {
        assertAll(
                () -> assertMatch(first.getProjectId(), second.getProjectId()),
                () -> assertEquals(first.getLocation(), second.getLocation()),
                () -> assertEquals(first.getRatingGroupId(), second.getRatingGroupId()),
                () -> assertEquals(first.getOfficeId(), second.getOfficeId()),
                () -> assertMatch(first.getCompoundOutletRecords(), second.getCompoundOutletRecords(), DTOMatch::assertMatch)
        );
    }

    public static void assertMatch(CompoundOutletRecord first, CompoundOutletRecord second) {
        assertAll(() -> assertMatch(first.getOutletId(), second.getOutletId()),
                  () -> assertEquals(first.getDownstreamOutletIds().size(), second.getDownstreamOutletIds().size()),
                  () -> assertAll(IntStream.range(0, first.getDownstreamOutletIds().size())
                                           .mapToObj(i -> () -> DTOMatch.assertMatch(
                                                   first.getDownstreamOutletIds().get(i),
                                                   second.getDownstreamOutletIds().get(i),
                                                   "Downstream Outlet Id " + i)))
        );
    }

    private static <T> void assertMatch(List<T> first, List<T> second, AssertMatchMethod<T> matcher) {
        assertAll(() -> assertEquals(first.size(), second.size()),
                  () -> assertAll(IntStream.range(0, first.size())
                                           .mapToObj(i -> () -> matcher.assertMatch(first.get(i), second.get(i)))));
    }

    @FunctionalInterface
    private interface AssertMatchMethod<T>{
        void assertMatch(T first, T second);
    }
}
