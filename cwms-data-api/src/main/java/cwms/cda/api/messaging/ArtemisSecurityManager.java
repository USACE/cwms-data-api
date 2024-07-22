/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.messaging;

import static cwms.cda.ApiServlet.CWMS_USERS_ROLE;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.security.CwmsAuthException;
import cwms.cda.security.DataApiPrincipal;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.activemq.artemis.core.security.CheckType;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

final class ArtemisSecurityManager implements ActiveMQSecurityManager {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private final DataSource dataSource;
    private final String cdaUser;

    ArtemisSecurityManager(DataSource dataSource) {
        this.dataSource = dataSource;
        cdaUser = DSL.using(dataSource, SQLDialect.ORACLE18C)
            .connectionResult(c -> c.getMetaData().getUserName());
    }

    @Override
    public boolean validateUser(String user, String password) {
        return validate(user, password);
    }

    @Override
    public boolean validateUserAndRole(String user, String password, Set<Role> roles, CheckType checkType) {
        //CDA User is allowed to send and manage messages for the invm acceptor.
        //Other users are not allowed to send messages.
        if (!cdaUser.equalsIgnoreCase(user) && (checkType == CheckType.SEND || checkType == CheckType.MANAGE)) {
            LOGGER.atWarning().log("User: " + user
                + " attempting to access Artemis Server with check type: " + checkType
                + " Only message consumption is supported.");
            return false;
        }
        return validate(user, password);
    }

    private boolean validate(String user, String password) {
        AuthDao instance = AuthDao.getInstance(DSL.using(dataSource, SQLDialect.ORACLE18C));
        boolean retval = false;
        try {
            DataApiPrincipal principal = instance.getByApiKey(password);
            retval = principal.getName().equalsIgnoreCase(user)
                && principal.getRoles().contains(new cwms.cda.security.Role(CWMS_USERS_ROLE));
        } catch (CwmsAuthException ex) {
            LOGGER.atWarning().withCause(ex).log("Unauthenticated user: " + user
                + " attempting to access Artemis Server");
        }
        return retval;
    }
}
