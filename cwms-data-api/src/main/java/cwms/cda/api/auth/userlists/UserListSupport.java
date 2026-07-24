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
import org.jooq.DSLContext;

final class UserListSupport {
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

    static DSLContext requireFeature(Context ctx) {
        DSLContext dsl = getDslContext(ctx);
        return UserListFeature.requireSupported(ctx, dsl) ? dsl : null;
    }

    static DataApiPrincipal principal(Context ctx) {
        return ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
    }

    static void requireOfficeAdmin(Context ctx, UserListDao dao, String office) {
        DataApiPrincipal principal = principal(ctx);
        if (principal == null || !dao.isOfficeUserAdmin(principal.getName(), office)) {
            throw new ForbiddenResponse("CWMS User Admins access is required for office " + office);
        }
    }
}
