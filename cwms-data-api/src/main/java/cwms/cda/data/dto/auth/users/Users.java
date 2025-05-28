package cwms.cda.data.dto.auth.users;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cwms.cda.data.dto.Clobs;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.Formats;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class Users extends CwmsDTOPaginated {
    List<User> users;


    private Users(String cursor, int pageSize, int total) {
        super(cursor, pageSize, total);
        users = new ArrayList<>();
    }

    public List<User> getClobs() {
        return Collections.unmodifiableList(users);
    }

    private void addUser(User user) {
        users.add(user);
    }

    public static class Builder {
        private final Users workingUsers;

        public Builder(String cursor, int pageSize, int total) {
            workingUsers = new Users(cursor, pageSize, total);
        }

        public Users build() {
            if (this.workingUsers.users.size() == this.workingUsers.pageSize && !this.workingUsers.users.isEmpty()) {
                User lastClob = this.workingUsers.users.get(this.workingUsers.users.size() - 1);
                String cursor = encodeCursor(CwmsDTOPaginated.delimiter, lastClob.getUserName());
                this.workingUsers.nextPage = encodeCursor(cursor, this.workingUsers.pageSize, this.workingUsers.total);
            } else {
                this.workingUsers.nextPage = null;
            }
            return workingUsers;
        }

        public Builder addUser(User user) {
            this.workingUsers.addUser(user);
            return this;
        }
    }
}
