package cwms.cda.api.auth.userlists;

import static cwms.cda.helpers.DatabaseHelpers.SCHEMA_VERSION.V2026_07_16;

import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.Dao;
import io.javalin.http.Context;
import java.net.HttpURLConnection;
import org.jooq.DSLContext;

public final class UserListFeature {
    public static final String UNSUPPORTED_MESSAGE =
            "User lists require CWMS database schema 26.07.16 or newer.";

    private UserListFeature() {
    }

    /**
     * Verifies that the connected CWMS schema supports user lists.
     *
     * @param ctx request context used to return an unsupported response
     * @param dsl office-scoped database context
     * @return true when user-list tables are available
     */
    public static boolean requireSupported(Context ctx, DSLContext dsl) {
        int version = Dao.versionAsInteger(Dao.getVersion(dsl));
        if (version < V2026_07_16.numeric()) {
            ctx.status(HttpURLConnection.HTTP_NOT_IMPLEMENTED)
                    .json(new CdaError(UNSUPPORTED_MESSAGE));
            return false;
        }
        return true;
    }
}
