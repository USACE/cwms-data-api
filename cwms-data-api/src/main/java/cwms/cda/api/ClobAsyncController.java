package cwms.cda.api;

import com.google.common.flogger.FluentLogger;
import cwms.cda.ApiServlet;
import cwms.cda.data.dao.JooqDao;
import io.javalin.http.Context;
import io.javalin.http.sse.SseClient;
import io.javalin.websocket.WsConfig;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

public class ClobAsyncController  {
    public static final FluentLogger logger = FluentLogger.forEnclosingClass();

    public Consumer<SseClient> getSseConsumer() {
        return this::accept;
    }

    public Consumer<WsConfig> getWsConsumer() {
        return this::acceptWs;
    }

    private void acceptWs(WsConfig ws) {

        ws.onConnect(ctx -> {
            logger.atInfo().log("#5--controller ws onConnect"); //#5 happens after #4

            String clobId = ctx.queryParam("id");
            String officeId= ctx.queryParam("officeId");

            Map<String, Object> servletAttributes = ctx.getUpgradeReq$javalin().getServletAttributes();

            Object ds = servletAttributes.get(ApiServlet.DATA_SOURCE);

            DSLContext dsl = DSL.using((DataSource) ds, SQLDialect.ORACLE18C);

//            sendClob(dsl, officeId, clobId, reader -> {
//                sendToClient(reader, ctx::send);
//            });





            new Thread(() -> {
                // For websockets this thread eventually does this, which might not be an error, it might just be what happens when the socket times out?
                // Notice that the clob is "42,303,148 bytes" according to the filesystem.
                // This could just be because I have jetty logging turned too high.
                // Jan 31, 2024 9:49:04 AM cwms.cda.api.ClobAsyncController sendToClient
                //INFO: Sending 32428 bytes to client. Total sent: 42303148
                //2024-01-31 09:49:04.000:DBUG:oejwc.WebSocketSession:Thread-15: [SERVER] WebSocketSession.getRemote()
                //2024-01-31 09:49:04.000:DBUG:oejwc.WebSocketRemoteEndpoint:Thread-15: sendStringByFuture with HeapByteBuffer@62df1514[p=0,l=32428,c=32428,r=32428]={<<<21-08-04_Batch_File_Utili...ort was removed in 8.0\n>>>}
                //2024-01-31 09:49:04.000:DBUG:oejwce.ExtensionStack:Thread-15: Queuing TEXT[len=32428,fin=true,rsv=...,masked=false]
                //2024-01-31 09:49:04.000:DBUG:oejwce.ExtensionStack:Thread-15: Processing TEXT[len=32428,fin=true,rsv=...,masked=false]
                //2024-01-31 09:49:04.000:DBUG:oejwcec.PerMessageDeflateExtension:Thread-15: nextOutgoingFrame(TEXT[len=2016,fin=true,rsv=1..,masked=false])
                //2024-01-31 09:49:04.000:DBUG:oejwci.AbstractWebSocketConnection:Thread-15: outgoingFrame(TEXT[len=2016,fin=true,rsv=1..,masked=false], org.eclipse.jetty.websocket.common.extensions.compress.CompressExtension$Flusher$1@1c316666)
                //2024-01-31 09:49:04.000:DBUG:oejwci.FrameFlusher:Thread-15: Enqueued FrameEntry[TEXT[len=2016,fin=true,rsv=1..,masked=false],org.eclipse.jetty.websocket.common.extensions.compress.CompressExtension$Flusher$1@1c316666,AUTO,null] to Flusher@352c2c26[IDLE][queueSize=1,aggregateSize=-1,terminated=null]
                //2024-01-31 09:49:04.000:DBUG:oejwci.FrameFlusher:Thread-15: Flushing Flusher@352c2c26[PROCESSING][queueSize=1,aggregateSize=-1,terminated=null]
                //2024-01-31 09:49:04.000:DBUG:oejwci.FrameFlusher:Thread-15: Flusher@352c2c26[PROCESSING][queueSize=0,aggregateSize=-1,terminated=null] processing 1 entries: [FrameEntry[TEXT[len=2016,fin=true,rsv=1..,masked=false],org.eclipse.jetty.websocket.common.extensions.compress.CompressExtension$Flusher$1@1c316666,AUTO,null]]
                //2024-01-31 09:49:04.001:DBUG:oejwci.FrameFlusher:Thread-15: Flusher@352c2c26[PROCESSING][queueSize=0,aggregateSize=0,terminated=null] acquired aggregate buffer java.nio.DirectByteBuffer[pos=0 lim=0 cap=32768]
                //2024-01-31 09:49:04.001:DBUG:oejwci.FrameFlusher:Thread-15: Flusher@352c2c26[PROCESSING][queueSize=0,aggregateSize=0,terminated=null] aggregated 1 frames: [FrameEntry[TEXT[len=0,fin=true,rsv=1..,masked=false],org.eclipse.jetty.websocket.common.extensions.compress.CompressExtension$Flusher$1@1c316666,AUTO,null]]
                //2024-01-31 09:49:04.001:DBUG:oejwci.FutureWriteCallback:Thread-15: .writeSuccess
                //2024-01-31 09:49:04.001:DBUG:oejwci.FrameFlusher:Thread-15: Flushing Flusher@352c2c26[PROCESSING][queueSize=0,aggregateSize=0,terminated=null]
                //2024-01-31 09:49:04.001:DBUG:oejwci.FrameFlusher:Thread-15: Flusher@352c2c26[PROCESSING][queueSize=0,aggregateSize=0,terminated=null] processing 0 entries: []
                //2024-01-31 09:49:04.001:DBUG:oejwci.FrameFlusher:Thread-15: Flusher@352c2c26[PROCESSING][queueSize=0,aggregateSize=0,terminated=null] auto flushing
                //2024-01-31 09:49:04.001:DBUG:oejwci.FrameFlusher:Thread-15: Flusher@352c2c26[PROCESSING][queueSize=0,aggregateSize=0,terminated=null] flushing aggregate java.nio.DirectByteBuffer[pos=0 lim=2020 cap=32768]
                //2024-01-31 09:49:04.001:DBUG:oejwci.FrameFlusher:Thread-15: Flusher@352c2c26[PROCESSING][queueSize=0,aggregateSize=0,terminated=null] flushing 0 frames: []
                //2024-01-31 09:49:04.001:DBUG:oejwci.FrameFlusher:Thread-15: Flushing Flusher@352c2c26[PROCESSING][queueSize=0,aggregateSize=2020,terminated=null]
                //2024-01-31 09:49:04.001:DBUG:oejwci.FrameFlusher:Thread-15: Flusher@352c2c26[PROCESSING][queueSize=0,aggregateSize=2020,terminated=null] processing 0 entries: []
                //2024-01-31 09:49:04.001:DBUG:oejwci.FrameFlusher:Thread-15: Flusher@352c2c26[PROCESSING][queueSize=0,aggregateSize=2020,terminated=null] auto flushing
                //2024-01-31 09:49:04.001:DBUG:oejwci.FrameFlusher:Thread-15: Flusher@352c2c26[PROCESSING][queueSize=0,aggregateSize=2020,terminated=null] flushing 0 frames: []
                //2024-01-31 09:49:04.001:DBUG:oejwce.ExtensionStack:Thread-15: Entering IDLE
                //2024-01-31 09:54:04.007:DBUG:oejwc.WebSocketSession:Connector-Scheduler-2141a12-1: callApplicationOnError()
                //org.eclipse.jetty.websocket.api.CloseException: java.util.concurrent.TimeoutException: Idle timeout expired: 300005/300000 ms
                //	at org.eclipse.jetty.websocket.common.io.AbstractWebSocketConnection.onReadTimeout(AbstractWebSocketConnection.java:564)
                //	at org.eclipse.jetty.io.AbstractConnection.onFillInterestedFailed(AbstractConnection.java:172)
                //	at org.eclipse.jetty.websocket.common.io.AbstractWebSocketConnection.onFillInterestedFailed(AbstractWebSocketConnection.java:539)
                //	at org.eclipse.jetty.io.AbstractConnection$ReadCallback.failed(AbstractConnection.java:317)
                //	at org.eclipse.jetty.io.FillInterest.onFail(FillInterest.java:140)
                //	at org.eclipse.jetty.io.AbstractEndPoint.onIdleExpired(AbstractEndPoint.java:407)
                //	at org.eclipse.jetty.io.IdleTimeout.checkIdleTimeout(IdleTimeout.java:171)
                //	at org.eclipse.jetty.io.IdleTimeout.idleCheck(IdleTimeout.java:113)
                //	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
                //	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
                //	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
                //	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
                //	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
                //	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
                //	at java.lang.Thread.run(Thread.java:748)
                //Caused by:
                //java.util.concurrent.TimeoutException: Idle timeout expired: 300005/300000 ms
                //	at org.eclipse.jetty.io.IdleTimeout.checkIdleTimeout(IdleTimeout.java:171)
                //	at org.eclipse.jetty.io.IdleTimeout.idleCheck(IdleTimeout.java:113)
                //	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
                //	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
                //	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$201(ScheduledThreadPoolExecutor.java:180)
                //	at java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:293)
                //	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
                //	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
                //	at java.lang.Thread.run(Thread.java:748)
                //2024-01-31 09:54:04.007:DBUG:oejwce.JettyAnnotatedEventDriver:Connector-Scheduler-2141a12-1: onError(org.eclipse.jetty.websocket.api.CloseException) - events.onError=OptionalSessionCallableMethod[pojo=io.javalin.websocket.WsConnection,method=public final void io.javalin.websocket.WsConnection.onError(org.eclipse.jetty.websocket.api.Session,java.lang.Throwable),wantsSession=true,streaming=false]
                //Jan 31, 2024 9:54:04 AM cwms.cda.api.ClobAsyncController lambda$acceptWs$5
                //INFO: ws onError
                sendClob(dsl, officeId, clobId, reader -> sendToClient(reader, ctx::send));
            }).start();
        });
        ws.onClose(ctx -> {
           logger.atInfo().log("ws onClose");
        });
        ws.onMessage(ctx -> {
            logger.atInfo().log("ws onMessage");
        });
        ws.onError(ctx -> {
            logger.atInfo().log("ws onError");
        });

    }

    public void accept(SseClient client) {
        String clobId = client.ctx.queryParam("id");
        String officeId= client.ctx.queryParam("officeId");

        logger.atInfo().log("got an sse clob request for:" + clobId );

        client.sendEvent("retry", "10000\n\n", null);  // could not get this to work...

        client.sendEvent("message", "Hello clob Client! Get ready for your clob!\n");
        client.onClose(() -> logger.atInfo().log("Clob client left."));

        // call sendClob on new thread
        new Thread(() -> sendClob(JooqDao.getDslContext(client.ctx), officeId, clobId, reader -> {

            sendToClient(reader, str -> client.sendEvent("message", str));
            client.sendEvent("close", "");  // normally the server just closing the connection would make the browser restart their side.
//            client.close();
        })).start();
    }

    private static void sendToClient( Reader reader, Consumer<String> strConsumer) {
        // read from reader in chunks and send to client
        char[] buffer = new char[32768];
        long total = 0;
        int bytesRead = 0;
        try {
            while (true) {
                if ((bytesRead = reader.read(buffer)) == -1) break;
                total += bytesRead;
                logger.atInfo().log("Sending " + bytesRead + " bytes to client. Total sent: " + total);
                strConsumer.accept(new String(buffer, 0, bytesRead));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    private static void sendClob(DSLContext dslContext, String officeId, String clobId, Consumer<Reader> readerConsumer) {

        dslContext.connection(
                connection -> {
                    String sql = "select cwms_20.AV_CLOB.VALUE from "
                            + "cwms_20.av_clob join cwms_20.av_office on av_clob.office_code = av_office.office_code "
                            + "where av_office.office_id = ? and av_clob.id = ?";
                    try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                        preparedStatement.setString(1, officeId);
                        preparedStatement.setString(2, clobId);

                        try (ResultSet resultSet = preparedStatement.executeQuery()) {
                            if (resultSet.next()) {
                                // Get the CLOB column
                                Clob clob = resultSet.getClob("VALUE");
                                if(clob != null) {
                                    // Open a Reader to stream CLOB data
                                    try (Reader reader = clob.getCharacterStream()) {
                                        if (reader != null) {
                                            readerConsumer.accept(reader);
                                        } else {
                                            logger.atInfo().log("clob.getCharacterStream returned null.");
                                        }

                                    }
                                } else {
                                    logger.atInfo().log("clob returned for " + clobId + " was null.");
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.atSevere().withCause(e).log("Error getting clob.");
                        throw new RuntimeException(e);
                    }
                }
        );
    }


    public static String getNowStr() {
        OffsetDateTime currentDateTimeWithOffset = OffsetDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z");
        return currentDateTimeWithOffset.format(formatter);
    }

    private static Optional<String> getUser(Context ctx) {
        Optional<String> retval = Optional.empty();
        if (ctx != null && ctx.req != null && ctx.req.getUserPrincipal() != null) {
            retval = Optional.of(ctx.req.getUserPrincipal().getName());
        } else {
            logger.atFine().log( "No user principal found in request.");
        }
        return retval;
    }

}
