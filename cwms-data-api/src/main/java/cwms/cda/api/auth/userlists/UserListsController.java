package cwms.cda.api.auth.userlists;

import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.GET_ALL;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.UserListDao;
import cwms.cda.data.dto.auth.userlists.UserList;
import cwms.cda.data.dto.auth.userlists.UserListInput;
import cwms.cda.data.dto.auth.userlists.UserLists;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.http.HttpCode;
import org.jooq.DSLContext;

public final class UserListsController implements Handler {
    private final MetricRegistry metrics;

    public UserListsController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @Override
    public void handle(Context ctx) {
        if ("GET".equals(ctx.method())) {
            getAll(ctx);
        } else {
            create(ctx);
        }
    }

    private void getAll(Context ctx) {
        try (Timer.Context ignored = Controllers.markAndTime(metrics, getClass().getName(), GET_ALL)) {
            String office = UserListSupport.requiredOffice(ctx);
            DSLContext dsl = UserListSupport.requireFeature(ctx);
            if (dsl == null) {
                return;
            }
            ctx.json(new UserLists(new UserListDao(dsl).getUserLists(office)));
        }
    }

    private void create(Context ctx) {
        try (Timer.Context ignored = Controllers.markAndTime(metrics, getClass().getName(), CREATE)) {
            UserListInput input = ctx.bodyAsClass(UserListInput.class);
            String office = input.getOfficeId();
            if (office == null || office.isBlank() || input.getUserListId() == null
                    || input.getUserListId().isBlank()) {
                throw new IllegalArgumentException("office-id and user-list-id are required");
            }
            DSLContext dsl = UserListSupport.requireFeature(ctx);
            if (dsl == null) {
                return;
            }
            UserListDao dao = new UserListDao(dsl);
            UserListSupport.requireOfficeAdmin(ctx, dao, office);
            UserList created = dao.createUserList(office, input.getUserListId(),
                    input.getDescription(), UserListSupport.principal(ctx).getName());
            ctx.status(HttpCode.CREATED).json(created);
        }
    }
}
