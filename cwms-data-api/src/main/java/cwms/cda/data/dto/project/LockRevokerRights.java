package cwms.cda.data.dto.project;

public class LockRevokerRights {
    private final String officeId;
    private final String projectId;
    private final String applicationId;
    private final String userId;

    public LockRevokerRights(String officeId, String projectId, String applicationId,
                             String userId) {
        this.officeId = officeId;
        this.projectId = projectId;
        this.applicationId = applicationId;
        this.userId = userId;
    }

    public String getOfficeId() {
        return officeId;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getUserId() {
        return userId;
    }
}
