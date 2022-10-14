package cwms.radar.test.security;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.Principal;
import java.time.ZonedDateTime;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

import cwms.auth.CwmsUserPrincipal;
import cwms.radar.security.Role;
import cwms.radar.spi.AccessManagerProvider;
import cwms.radar.spi.RadarAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.In;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

/**
 * An extremely simple implementation of an access manager for testing writes
 * and other use cases
 */
public class TestAccessManagerProvider implements AccessManagerProvider {

    @Override
    public String getName() {
        return "TEST";
    }

    @Override
    public RadarAccessManager create() {
        return new RadarAccessManager(){

            private final ArrayList<CwmsUserPrincipal> users = new ArrayList<>();
            
            /**
             * @throws IOException
             */
            private void loadUsers() throws IOException {
                
                List<String> data = IOUtils.readLines(
                                        this.getClass()
                                            .getClassLoader()
                                            .getResourceAsStream("cwms/radar/security/testusers.csv"),
                                        Charset.forName("UTF-8"))
                                        ;
                for(String dataLine: data) {
                    final String parts[] = dataLine.split(",");
                    users.add( new CwmsUserPrincipal() {
                        
                        @Override
                        public String getName() {
                            return parts[0];
                        }

                        @Override
                        public String getEdipi() {
                            return null;
                        }

                        @Override
                        public String getSessionKey() {
                            return null;
                        }

                        @Override
                        public String getSessionUUID() {
                            return null;
                        }

                        @Override
                        public ZonedDateTime getLastLogin() {
                            return null;
                        }

                        @Override
                        public List<String> getRoles() {
                            ArrayList<String> roles = new ArrayList<>();
                            for(String role: parts[1].split("|")) {
                                roles.add(role);
                            }
                            return roles;
                        }
                        
                    });
                }
            }

            @Override
            public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
                if( users.isEmpty() ) {
                    loadUsers();
                }

                boolean retval;
                if ( routeRoles == null || routeRoles.isEmpty() || getRoles(ctx).containsAll(routeRoles))
                     {
                     handler.handle(ctx);
                }
        
                
        
            }

            private Set<RouteRole> getRoles(Context ctx) {
                Set<RouteRole> roles = new HashSet<>();;
                String user = ctx.header("Authentication").split(":")[1].split(" ")[1];
                for( CwmsUserPrincipal p: users){
                    if( p.getName().equals(user)) {
                        p.getRoles().stream().map(Role::new).forEach(roles::add);
                        break;
                    }
                }
                return roles;
            }

            @Override
            public SecurityScheme getScheme() {
                return new SecurityScheme()
					.type(Type.APIKEY)
					.in(In.HEADER)
					.name("Authentication:");
            }

        };
    }
    
}
