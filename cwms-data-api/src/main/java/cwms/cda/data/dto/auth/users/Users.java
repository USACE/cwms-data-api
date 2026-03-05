package cwms.cda.data.dto.auth.users;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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
        this.users.add(user);
    }

    private void addUsers(List<User> users) {
        this.users.addAll(users);
    }

    public static class Builder {
        private final Users workingUsers;
        private final Optional<String> nextPage;
        private final String limitOffice;
        private final String userNameRegex;


        public Builder(String cursor, int pageSize, int total, String limitOffice, String userNameRegex) {
            workingUsers = new Users(cursor, pageSize, total);
            this.nextPage = Optional.empty();
            this.limitOffice = limitOffice;
            this.userNameRegex = userNameRegex;
        }
        
        /**
         * Used when deserializing provided JSON.
         * @param cursor current page
         * @param nextPage next-page the next page or null
         * @param pageSize page size
         * @param total total for the entire set
         */
        @JsonCreator
        public Builder(@JsonProperty("page") String cursor,
                       @JsonProperty("next-page") String nextPage,
                       @JsonProperty("page-size") int pageSize,
                       @JsonProperty("total") int total) {
            workingUsers = new Users(cursor, pageSize, total);
            this.nextPage = Optional.of(nextPage != null ? nextPage : "end");
            this.limitOffice = null; // Not used when processing existing JSON, value is encoded in next-page for the query.
            this.userNameRegex = null; // Not used when processing existing JSON, value is encoded in next-page for the query.
        }

        public Users build() {
            if (this.nextPage.isPresent()) {
                final String next = this.nextPage.get();
                workingUsers.nextPage = !next.equals("end") ? next : null;
            }
            else if (this.workingUsers.users.size() == this.workingUsers.pageSize && !this.workingUsers.users.isEmpty()) {
                User lastUser = this.workingUsers.users.get(this.workingUsers.users.size() - 1);
                UsersPageCursor pageCursor = new UsersPageCursor.Builder(lastUser.getUserName(), this.workingUsers.pageSize, this.workingUsers.total)
                        .withLimitOffice(this.limitOffice)
                        .withUsernameRegex(this.userNameRegex)
                        .build();
                this.workingUsers.nextPage = CwmsDTOPaginated.encodeCursor(pageCursor);

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
