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

package cwms.cda.data.dao.project;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.project.ProjectChildren;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.codegen.tables.AV_EMBANKMENT;
import usace.cwms.db.jooq.codegen.tables.AV_GATE;
import usace.cwms.db.jooq.codegen.tables.AV_LOCK;
import usace.cwms.db.jooq.codegen.tables.AV_OUTLET;
import usace.cwms.db.jooq.codegen.tables.AV_TURBINE;

public class ProjectChildrenDao extends JooqDao<ProjectChildren> {

    public ProjectChildrenDao(DSLContext dsl) {
        super(dsl);
    }


    public List<ProjectChildren> children(String office, String projLike, String kindRegex) {

        List<String> allKinds = getProjectKinds();

        Set<String> kinds = getMatchingKinds(kindRegex, allKinds);

        return children(office, projLike, kinds);
    }

    private List<ProjectChildren> children(String office, String projLike, Set<String> kinds) {

        Map<String, ProjectChildren.Builder> builderMap = new LinkedHashMap<>();  // proj-id->

        for (String kind : kinds) {
            Map<String, List<CwmsId>> locsOfKind = getChildrenOfKind(office, projLike, kind);
            if (locsOfKind != null) {
                for (Map.Entry<String, List<CwmsId>> entry : locsOfKind.entrySet()) {
                    String projId = entry.getKey();
                    List<CwmsId> locs = entry.getValue();
                    ProjectChildren.Builder builder = builderMap.computeIfAbsent(projId, k ->
                            new ProjectChildren.Builder()
                                    .withProject(new CwmsId.Builder()
                                            .withOfficeId(office)
                                            .withName(projId)
                                            .build()));
                    switch (kind) {
                        case "EMBANKMENT":
                            builder.withEmbankments(locs);
                            break;
                        case "LOCK":
                            builder.withLocks(locs);
                            break;
                        case "OUTLET":
                            builder.withOutlets(locs);
                            break;
                        case "TURBINE":
                            builder.withTurbines(locs);
                            break;
                        case "GATE":
                            builder.withGates(locs);
                            break;
                        default:
                            break;
                    }

                }
            }
        }

        return builderMap.values().stream()
                .map(ProjectChildren.Builder::build)
                .collect(Collectors.toList());

    }

    @Nullable
    private Map<String, List<CwmsId>> getChildrenOfKind(String office, String projLike, String kind) {
        switch (kind) {

            case "EMBANKMENT":
                return getEmbankmentChildren(office, projLike);
            case "TURBINE":
                return getTurbineChildren(office, projLike);
            case "OUTLET":
                return getOutletChildren(office, projLike);
            case "LOCK":
                return getLockChildren(office, projLike);
            case "GATE":
                return getGateChildren(office, projLike);
            default:
                return null;
        }

    }

    private Map<String, List<CwmsId>> getGateChildren(String office, @Nullable String projLike) {
        Map<String, List<CwmsId>> retval = new LinkedHashMap<>();
        AV_GATE view = AV_GATE.AV_GATE;
        dsl.selectDistinct(view.OFFICE_ID, view.GATE_ID, view.PROJECT_ID)
                .from(view)
                .where(view.OFFICE_ID.eq(office))
                .and(caseInsensitiveLikeRegexNullTrue(view.PROJECT_ID, projLike))
                .orderBy(view.OFFICE_ID, view.PROJECT_ID, view.GATE_ID)
                .forEach(row -> {
                    String projId = row.get(view.PROJECT_ID);
                    CwmsId embankment = new CwmsId.Builder()
                            .withOfficeId(row.get(view.OFFICE_ID))
                            .withName(row.get(view.GATE_ID))
                            .build();
                    retval.computeIfAbsent(projId, k -> new ArrayList<>()).add(embankment);
                });


        return retval;
    }

    private Map<String, List<CwmsId>> getLockChildren(String office, String projLike) {
        Map<String, List<CwmsId>> retval = new LinkedHashMap<>();
        AV_LOCK view = AV_LOCK.AV_LOCK;
        dsl.selectDistinct(view.DB_OFFICE_ID, view.LOCK_ID, view.PROJECT_ID)
                .from(view)
                .where(view.DB_OFFICE_ID.eq(office))
                .and(caseInsensitiveLikeRegexNullTrue(view.PROJECT_ID, projLike))
                .orderBy(view.DB_OFFICE_ID, view.PROJECT_ID, view.LOCK_ID)
                .forEach(row -> {
                    String projId = row.get(view.PROJECT_ID);
                    CwmsId embankment = new CwmsId.Builder()
                            .withOfficeId(row.get(view.DB_OFFICE_ID))
                            .withName(row.get(view.LOCK_ID))
                            .build();
                    retval.computeIfAbsent(projId, k -> new ArrayList<>()).add(embankment);
                });


        return retval;
    }

    private Map<String, List<CwmsId>> getOutletChildren(String office, String projLike) {
        Map<String, List<CwmsId>> retval = new LinkedHashMap<>();
        AV_OUTLET view = AV_OUTLET.AV_OUTLET;
        dsl.selectDistinct(view.OFFICE_ID, view.OUTLET_ID, view.PROJECT_ID)
                .from(view)
                .where(view.OFFICE_ID.eq(office))
                .and(caseInsensitiveLikeRegexNullTrue(view.PROJECT_ID, projLike))
                .orderBy(view.OFFICE_ID, view.PROJECT_ID, view.OUTLET_ID)
                .forEach(row -> {
                    String projId = row.get(view.PROJECT_ID);
                    CwmsId embankment = new CwmsId.Builder()
                            .withOfficeId(row.get(view.OFFICE_ID))
                            .withName(row.get(view.OUTLET_ID))
                            .build();
                    retval.computeIfAbsent(projId, k -> new ArrayList<>()).add(embankment);
                });


        return retval;
    }

    private Map<String, List<CwmsId>> getTurbineChildren(String office, String projLike) {
        Map<String, List<CwmsId>> retval = new LinkedHashMap<>();
        AV_TURBINE view = AV_TURBINE.AV_TURBINE;
        dsl.selectDistinct(view.OFFICE_ID, view.TURBINE_ID, view.PROJECT_ID)
                .from(view)
                .where(view.OFFICE_ID.eq(office))
                .and(caseInsensitiveLikeRegexNullTrue(view.PROJECT_ID, projLike))
                .orderBy(view.OFFICE_ID, view.PROJECT_ID, view.TURBINE_ID)
                .forEach(row -> {
                    String projId = row.get(view.PROJECT_ID);
                    CwmsId embankment = new CwmsId.Builder()
                            .withOfficeId(row.get(view.OFFICE_ID))
                            .withName(row.get(view.TURBINE_ID))
                            .build();
                    retval.computeIfAbsent(projId, k -> new ArrayList<>()).add(embankment);
                });


        return retval;
    }

    private Map<String, List<CwmsId>> getEmbankmentChildren(String office, String projLike) {
        Map<String, List<CwmsId>> retval = new LinkedHashMap<>();
        AV_EMBANKMENT view = AV_EMBANKMENT.AV_EMBANKMENT;
        dsl.selectDistinct(view.OFFICE_ID, view.EMBANKMENT_LOCATION_ID, view.PROJECT_ID)
                .from(view)
                .where(view.OFFICE_ID.eq(office)
                        .and(view.UNIT_SYSTEM.eq("SI")))
                .and(caseInsensitiveLikeRegexNullTrue(view.PROJECT_ID, projLike))
                .orderBy(view.OFFICE_ID, view.PROJECT_ID, view.EMBANKMENT_LOCATION_ID)
                .forEach(row -> {
                    String projId = row.get(view.PROJECT_ID);
                    CwmsId embankment = new CwmsId.Builder()
                            .withOfficeId(row.get(view.OFFICE_ID))
                            .withName(row.get(view.EMBANKMENT_LOCATION_ID))
                            .build();
                    retval.computeIfAbsent(projId, k -> new ArrayList<>()).add(embankment);
                });


        return retval;
    }

    private static @NotNull List<String> getProjectKinds() {
        // Maybe this should be an enum?  Maybe just the Project types.
        // "SITE", "EMBANKMENT", "OVERFLOW", "TURBINE", "STREAM", "PROJECT",
        // "STREAMGAGE", "BASIN", "OUTLET", "LOCK", "GATE"
        String [] kinds = { "EMBANKMENT",  "TURBINE", "OUTLET", "LOCK", "GATE"};
        return Arrays.asList(kinds);
    }

    private Set<String> getMatchingKinds(String regex, List<String> allKinds) {
        Set<String> kinds = new LinkedHashSet<>();

        if (regex == null) {
            kinds.addAll(allKinds);
        } else {
            Pattern p = Pattern.compile(regex);
            for (String kind : allKinds) {
                Matcher matcher = p.matcher(kind);
                if (matcher.matches()) {
                    kinds.add(kind);
                }
            }
        }

        return kinds;
    }
}
