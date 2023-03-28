package fixtures.tomcat;

import java.io.IOException;

import org.apache.catalina.Session;
import org.apache.catalina.session.ManagerBase;

public class TestSessionManager extends ManagerBase {

    @Override
    public void load() throws ClassNotFoundException, IOException {
        
    }

    @Override
    public void unload() throws IOException {
        
    }

    public void addSession(Session session) {
        this.add(session);
    }
    
}
