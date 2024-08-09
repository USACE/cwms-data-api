package cwms.cda.data.dao.project;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Location Kinds that have a project as a parent.
 */
public enum ProjectKind {
    EMBANKMENT, TURBINE, OUTLET, LOCK, GATE;

    public static Set<ProjectKind> getMatchingKinds(String regex) {
        Set<ProjectKind> kinds = new LinkedHashSet<>();

        ProjectKind[] projectKinds = ProjectKind.values();
        if (regex == null) {
            kinds.addAll(Arrays.asList(projectKinds));
        } else {
            Pattern p = Pattern.compile(regex);
            for (ProjectKind kind : projectKinds) {
                Matcher matcher = p.matcher(kind.name());
                if (matcher.matches()) {
                    kinds.add(kind);
                }
            }
        }

        return kinds;
    }
}
