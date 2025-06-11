package cwms.cda.data.dto.auth.users;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import cwms.cda.formatters.json.JsonV2;
import io.swagger.v3.oas.annotations.media.Schema;
import cwms.cda.formatters.Formats;

@JsonRootName("users")
@JsonDeserialize(builder = Users.Builder.class)
@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class Users extends CwmsDTOPaginated {
    @JacksonXmlElementWrapper
    @JacksonXmlProperty(localName = "user")
    // Use the array shape to optimize data transfer to client
    //@JsonFormat(shape=JsonFormat.Shape.ARRAY)
    @Schema(description = "List of retrieved users")
    List<User> users;


    private Users(String cursor, int pageSize, int total) {
        super(cursor, pageSize, total);
        users = new ArrayList<>();
    }
   

    public List<User> getUsers() {
        return Collections.unmodifiableList(users);
    }

    private void addUser(User user) {
        users.add(user);
    }

    private void addUsers(List<User> users) {
        users.addAll(users);
    }

    public static class Builder {
        private final Users workingUsers;

        @JsonCreator
        public Builder(@JsonProperty("page") String cursor,
                       @JsonProperty("page-size") int pageSize,
                       @JsonProperty("total") int total) {
            workingUsers = new Users(cursor, pageSize, total);
        }

        public Users build() {
            if (this.workingUsers.users.size() == this.workingUsers.pageSize && !this.workingUsers.users.isEmpty()) {
                User lastUser = this.workingUsers.users.get(this.workingUsers.users.size() - 1);
                String cursor = encodeCursor(CwmsDTOPaginated.delimiter, lastUser.getUserName());
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

        @JsonSetter("users")
        public Builder addUsers(List<User> users) {
            this.workingUsers.addUsers(users);
            return this;
        }
    }
}
