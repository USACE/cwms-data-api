package cwms.cda.api.auth.userlists;

import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import cwms.cda.api.errors.RequiredQueryParameterException;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.UserListDao;
import cwms.cda.data.dto.Office;
import cwms.cda.security.DataApiPrincipal;
import io.javalin.http.Context;
import io.javalin.http.ForbiddenResponse;
import java.util.Locale;
import java.util.regex.Pattern;
import org.jooq.DSLContext;

final class UserListSupport {
    static final int USER_LIST_ID_MAX_LENGTH = 128;
    static final int DESCRIPTION_MAX_LENGTH = 1024;
    static final int USER_ID_MAX_LENGTH = 128;
    private static final Pattern USER_LIST_ID_PATTERN =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");

    private UserListSupport() {
    }

    static String requiredOffice(Context ctx) {
        String office = ctx.queryParam(OFFICE);
        if (office == null || office.isBlank()) {
            throw new RequiredQueryParameterException(OFFICE);
        }
        return ctx.queryParamAsClass(OFFICE, String.class)
                .check(Office::validOfficeNotNull, "Invalid office provided")
                .get();
    }

    static String validateOffice(String office) {
        if (office == null || office.isBlank()) {
            throw new IllegalArgumentException("office-id is required");
        }
        String normalized = office.strip().toUpperCase(Locale.ROOT);
        if (!Office.validOfficeNotNull(normalized)) {
            throw new IllegalArgumentException("Invalid office provided");
        }
        return normalized;
    }

    static DSLContext requireFeature(Context ctx) {
        return requireFeature(ctx, null);
    }

    static DSLContext requireFeature(Context ctx, String office) {
        DSLContext dsl = getDslContext(ctx, office);
        return UserListFeature.requireSupported(ctx, dsl) ? dsl : null;
    }

    static DataApiPrincipal principal(Context ctx) {
        return ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
    }

    static String validateUserListId(String userListId) {
        if (userListId == null || userListId.isBlank()) {
            throw new IllegalArgumentException("user-list-id is required");
        }
        String normalized = userListId.strip().toUpperCase(Locale.ROOT);
        if (!USER_LIST_ID_PATTERN.matcher(normalized).matches()) {
            throw new IllegalArgumentException("user-list-id must be 1-"
                    + USER_LIST_ID_MAX_LENGTH
                    + " characters and contain only letters, numbers, '.', '_' or '-'");
        }
        return normalized;
    }

    static String validateDescription(String description) {
        if (description == null) {
            return null;
        }
        String normalized = description.strip();
        if (normalized.length() > DESCRIPTION_MAX_LENGTH) {
            throw new IllegalArgumentException("description must not exceed "
                    + DESCRIPTION_MAX_LENGTH + " characters");
        }
        return normalized.isEmpty() ? null : normalized;
    }

    static String validateUserId(String userId) {
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("user-id is required");
        }
        String normalized = userId.strip().toUpperCase(Locale.ROOT);
        if (normalized.length() > USER_ID_MAX_LENGTH) {
            throw new IllegalArgumentException("user-id must not exceed "
                    + USER_ID_MAX_LENGTH + " characters");
        }
        return normalized;
    }

    static String validateCandidateSearch(String search) {
        if (search == null || search.isBlank() || search.strip().length() < 2) {
            throw new IllegalArgumentException("search must contain at least 2 characters");
        }
        String normalized = search.strip();
        if (normalized.length() > USER_ID_MAX_LENGTH) {
            throw new IllegalArgumentException("search must not exceed "
                    + USER_ID_MAX_LENGTH + " characters");
        }
        return normalized;
    }

    static void requireOfficeAdmin(Context ctx, UserListDao dao, String office) {
        DataApiPrincipal principal = principal(ctx);
        if (principal == null || !dao.isOfficeUserAdmin(principal.getName(), office)) {
            throw new ForbiddenResponse("CWMS User Admins access is required for office " + office);
        }
    }
}
