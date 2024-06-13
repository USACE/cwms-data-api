package cwms.cda.data.dto.project;

public class Lock {
    private final String officeId;
    private final String projectId;
    private final String applicationId;
    private final String acquireTime;
    private final String sessionUser;
    private final String osUser;
    private final String sessionProgram;
    private final String sessionMachine;

    public Lock(String officeId, String projectId, String applicationId, String acquireTime,
                String sessionUser, String osUser, String sessionProgram,
                String sessionMachine) {
        this.officeId = officeId;
        this.projectId = projectId;
        this.applicationId = applicationId;
        this.acquireTime = acquireTime;
        this.sessionUser = sessionUser;
        this.osUser = osUser;
        this.sessionProgram = sessionProgram;
        this.sessionMachine = sessionMachine;
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

    public String getAcquireTime() {
        return acquireTime;
    }

    public String getSessionUser() {
        return sessionUser;
    }

    public String getOsUser() {
        return osUser;
    }

    public String getSessionProgram() {
        return sessionProgram;
    }

    public String getSessionMachine() {
        return sessionMachine;
    }
}
