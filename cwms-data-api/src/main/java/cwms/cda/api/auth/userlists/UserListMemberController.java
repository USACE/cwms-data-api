package cwms.cda.api.auth.userlists;

import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.USER_ID;
import static cwms.cda.api.Controllers.USER_LIST_ID;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.UserListDao;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpCode;
import org.jooq.DSLContext;

public final class UserListMemberController implements Handler {
    private final MetricRegistry metrics;

    public UserListMemberController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @Override
    public void handle(Context ctx) {
        try (Timer.Context ignored = Controllers.markAndTime(
                metrics, getClass().getName(), DELETE)) {
            String office = UserListSupport.requiredOffice(ctx);
            DSLContext dsl = UserListSupport.requireFeature(ctx);
            if (dsl == null) {
                return;
            }
            UserListDao dao = new UserListDao(dsl);
            UserListSupport.requireOfficeAdmin(ctx, dao, office);
            dao.removeMember(office, ctx.pathParam(USER_LIST_ID), ctx.pathParam(USER_ID));
            ctx.status(HttpCode.NO_CONTENT);
        }
    }
}
