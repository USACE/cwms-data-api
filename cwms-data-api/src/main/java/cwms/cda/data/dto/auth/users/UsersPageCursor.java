package cwms.cda.data.dto.auth.users;

import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.PageCursor;

public final class UsersPageCursor implements PageCursor {
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
    public void decodeCursor(String cursor, String delimeter) {
        String[] decoded = CwmsDTOPaginated.decodeCursor(cursor, delimeter);
        if(decoded.length == 5) {
            cursorUserId = decoded[0];
            total = Integer.parseInt(decoded[2]);
            pageSize = Integer.parseInt(decoded[1]);
            limitOffice = decoded[3].equals("null") ? null : decoded[3];
            usernameRegex = decoded[4].equals("null") ? null : decoded[4];
        } else {
            throw new IllegalArgumentException("Invalid cursor format");
        }
    }
}
