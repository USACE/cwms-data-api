package fixtures;

import org.eclipse.jetty.ee10.servlet.SessionHandler;
import org.eclipse.jetty.session.ManagedSession;
import org.eclipse.jetty.session.SessionData;

import io.opentelemetry.sdk.trace.internal.shaded.jctools.queues.MessagePassingQueue.Consumer;

public class TestSessionHandler extends SessionHandler {
    
    public void createTestSession(String sessionId, Consumer<ManagedSession> consumer) throws Exception {
        var cache = this.getSessionCache();
        
        if (!cache.contains(sessionId)) {
            var session = new ManagedSession(this, new SessionData(sessionId, "/" , "localhost", 
                                             System.currentTimeMillis(), System.currentTimeMillis(),
                                             System.currentTimeMillis(), getMaxInactiveInterval()));
            
            cache.add(sessionId, session);
            consumer.accept(session);
        }
    }
}
