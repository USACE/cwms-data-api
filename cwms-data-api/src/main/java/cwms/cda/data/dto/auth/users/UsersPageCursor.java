package cwms.cda.data.dto.auth.users;

import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.PageCursor;

import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Collectors;

import static cwms.cda.data.dto.PageCursor.decodeNullableField;
import static cwms.cda.data.dto.PageCursor.encodeNullableField;

public final class UsersPageCursor implements PageCursor {
    private static final int CURSOR_USER_ID_INDEX = 0;
    private static final int PAGE_SIZE_INDEX = 1;
    private static final int TOTAL_INDEX = 2;
    private static final int LIMIT_OFFICE_INDEX = 3;
    private static final int USERNAME_REGEX_INDEX = 4;
    private static final int EXPECTED_CURSOR_PARTS = 5;
    String cursorUserId;
    int total;
    int pageSize;
    String limitOffice;
    String usernameRegex;

    public String getCursorUserId() {
        return cursorUserId;
    }

    public int getTotal() {
        return total;
    }

    public int getPageSize() {
        return pageSize;
    }

    public String getLimitOffice() {
        return limitOffice;
    }

    public String getUsernameRegex() {
        return usernameRegex;
    }

    @Override
    public void decodeCursor(String cursor, String delimiter) {
        String[] decoded = CwmsDTOPaginated.decodeCursor(cursor, delimiter);
        if(decoded.length == EXPECTED_CURSOR_PARTS) {
            cursorUserId = decoded[CURSOR_USER_ID_INDEX];
            pageSize = Integer.parseInt(decoded[PAGE_SIZE_INDEX]);
            total = Integer.parseInt(decoded[TOTAL_INDEX]);
            limitOffice = decodeNullableField(decoded[LIMIT_OFFICE_INDEX]);
            usernameRegex = decodeNullableField(decoded[USERNAME_REGEX_INDEX]);
        } else {
            throw new IllegalArgumentException("Invalid cursor format");
        }
    }

    @Override
    public String encode(Base64.Encoder encoder, String delimiter) {
        Object[] cursorParts = new Object[EXPECTED_CURSOR_PARTS];
        cursorParts[CURSOR_USER_ID_INDEX] = cursorUserId;
        cursorParts[PAGE_SIZE_INDEX] = pageSize;
        cursorParts[TOTAL_INDEX] = total;
        cursorParts[LIMIT_OFFICE_INDEX] = encodeNullableField(limitOffice);
        cursorParts[USERNAME_REGEX_INDEX] = encodeNullableField(usernameRegex);
        return (cursorUserId == null || cursorUserId.equals("*")) ? null :
                encoder.encodeToString(Arrays.stream(cursorParts)
                    .map(String::valueOf)
                    .collect(Collectors.joining(delimiter))
                    .getBytes());

    }

    public static class Builder {
        private String cursorUserId;
        private int total;
        private int pageSize;
        private String limitOffice;
        private String usernameRegex;

        public Builder withCursorUserId(String cursorUserId) {
            this.cursorUserId = cursorUserId;
            return this;
        }

        public Builder withTotal(int total) {
            this.total = total;
            return this;
        }

        public Builder withPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder withLimitOffice(String limitOffice) {
            this.limitOffice = limitOffice;
            return this;
        }

        public Builder withUsernameRegex(String usernameRegex) {
            this.usernameRegex = usernameRegex;
            return this;
        }

        public UsersPageCursor build() {
            UsersPageCursor cursor = new UsersPageCursor();
            cursor.cursorUserId = this.cursorUserId;
            cursor.total = this.total;
            cursor.pageSize = this.pageSize;
            cursor.limitOffice = this.limitOffice;
            cursor.usernameRegex = this.usernameRegex;
            return cursor;
        }
    }
}
