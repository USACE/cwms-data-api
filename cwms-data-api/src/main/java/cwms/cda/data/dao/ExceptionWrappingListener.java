package cwms.cda.data.dao;

import org.jooq.ExecuteContext;
import org.jooq.impl.DefaultExecuteListener;

class ExceptionWrappingListener extends DefaultExecuteListener {

    @Override
    public void exception(ExecuteContext ctx) {
        super.exception(ctx);

        RuntimeException exception = JooqDao.wrapException(ctx.exception());

        ctx.exception(exception);
    }
}
