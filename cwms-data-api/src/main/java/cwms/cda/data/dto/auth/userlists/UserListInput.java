package cwms.cda.data.dto.auth.userlists;

public final class UserListInput {
    private String officeId;
    private String userListId;
    private String description;

    public UserListInput() {
    }

    public String getOfficeId() {
        return officeId;
    }

    public void setOfficeId(String officeId) {
        this.officeId = officeId;
    }

    public String getUserListId() {
        return userListId;
    }

    public void setUserListId(String userListId) {
        this.userListId = userListId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
