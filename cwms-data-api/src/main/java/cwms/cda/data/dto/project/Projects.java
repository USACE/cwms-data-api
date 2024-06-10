package cwms.cda.data.dto.project;

import cwms.cda.api.errors.FieldException;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.Location;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


@FormattableWith(contentType = Formats.JSON, formatter = JsonV2.class)
public class Projects extends CwmsDTOPaginated {

    List<Project> projects;

    private Projects() {
    }

    public Projects(String page, int pageSize, Integer total) {
        super(page, pageSize, total);
        projects = new ArrayList<>();
    }

    public List<Project> getProjects() {
        return Collections.unmodifiableList(projects);
    }


    /**
     * Extract the office from the cursor.
     *
     * @param cursor the cursor
     * @return office
     */
    public static String getOffice(String cursor) {
        String[] parts = CwmsDTOPaginated.decodeCursor(cursor);
        if (parts.length > 1) {
            String[] idAndOffice = CwmsDTOPaginated.decodeCursor(parts[0]);
            if (idAndOffice.length > 0) {
                return idAndOffice[0];
            }
        }
        return null;
    }

    /**
     * Extract the id from the cursor.
     *
     * @param cursor the cursor
     * @return id
     */
    public static String getId(String cursor) {
        String[] parts = CwmsDTOPaginated.decodeCursor(cursor);
        if (parts.length > 1) {
            String[] idAndOffice = CwmsDTOPaginated.decodeCursor(parts[0]);
            if (idAndOffice.length > 1) {
                return idAndOffice[1];
            }
        }
        return null;
    }

    public static int getPageSize(String cursor) {
        String[] parts = CwmsDTOPaginated.decodeCursor(cursor);
        if (parts.length > 2) {
            return Integer.parseInt(parts[2]);
        }
        return 0;
    }

    public static int getTotal(String cursor) {
        String[] parts = CwmsDTOPaginated.decodeCursor(cursor);
        if (parts.length > 1) {
            return Integer.parseInt(parts[1]);
        }
        return 0;
    }


    public static class Builder {
        private Projects workingProjects;

        public Builder(String currentPage, int pageSize, Integer total) {
            workingProjects = new Projects(currentPage, pageSize, total);
        }

        public Projects build() {
            if (this.workingProjects.projects.size() == this.workingProjects.pageSize) {
                Project last =
                        this.workingProjects.projects.get(this.workingProjects.projects.size() - 1);
                Location lastLoc = last.getLocation();
                String cursor = encodeCursor(CwmsDTOPaginated.delimiter, lastLoc.getOfficeId(),
                        lastLoc.getName());
                this.workingProjects.nextPage = encodeCursor(cursor,
                        this.workingProjects.pageSize, this.workingProjects.total);
            } else {
                this.workingProjects.nextPage = null;
            }
            return workingProjects;
        }

        public Builder add(Project project) {
            this.workingProjects.projects.add(project);
            return this;
        }

        public Builder addAll(Collection<? extends Project> projects) {
            this.workingProjects.projects.addAll(projects);
            return this;
        }
    }


    @Override
    public void validate() throws FieldException {
        // TODO Auto-generated method stub

    }


}
