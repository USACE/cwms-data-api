package helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import fixtures.CwmsDataApiSetupCallback;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;

/** Measures a schema change only inside the owned, disposable benchmark database. */
final class SessionSetupBenchmark {
    private SessionSetupBenchmark() {
    }

    static void run(Path directory) throws Exception {
        CwmsDatabaseContainer<?> database = CwmsDataApiSetupCallback.getDatabaseLink();
        if (database == null || !database.isRunning()) {
            throw new IllegalStateException("An owned running test container is required");
        }
        Files.createDirectories(directory);
        List<Map<String, Object>> results = new ArrayList<>();
        withConnection(database, "cwms_20", owner -> {
            String original = packageSource(owner);
            String patched = withoutSuccessLog(original);
            try {
                withConnection(database, "sys", observer -> {
                    for (int variant = 0; variant < 3; variant++) {
                        boolean logging = variant != 1;
                        install(owner, logging ? original : patched);
                        for (int sample = 0; sample < 5; sample++) {
                            results.add(measure(database, observer, variant, logging, sample));
                        }
                    }
                });
            } finally {
                install(owner, original);
            }
        });
        new ObjectMapper().writerWithDefaultPrettyPrinter()
                .writeValue(directory.resolve("session-setup.json").toFile(), results);
        System.out.println("Session setup logging comparison complete: " + results.size() + " fresh sessions");
    }

    static void disableSuccessLogging() throws Exception {
        CwmsDatabaseContainer<?> database = CwmsDataApiSetupCallback.getDatabaseLink();
        if (database == null || !database.isRunning()) {
            throw new IllegalStateException("An owned running test container is required");
        }
        withConnection(database, "cwms_20", owner -> {
            install(owner, withoutSuccessLog(packageSource(owner)));
        });
    }

    private static void withConnection(CwmsDatabaseContainer<?> database, String user, SqlAction action)
            throws SQLException {
        database.connection(connection -> {
            try {
                action.run(connection);
            } catch (SQLException ex) {
                throw new IllegalStateException("Session setup probe failed", ex);
            }
        }, user);
    }

    @FunctionalInterface
    private interface SqlAction {
        void run(Connection connection) throws SQLException;
    }

    private static String packageSource(Connection connection) throws SQLException {
        StringBuilder source = new StringBuilder();
        try (PreparedStatement query = connection.prepareStatement(
                "select text from user_source where name = ? and type = ? order by line")) {
            query.setString(1, "CWMS_ENV");
            query.setString(2, "PACKAGE BODY");
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    source.append(rows.getString(1));
                }
            }
        }
        if (source.length() == 0) {
            throw new IllegalStateException("CWMS_ENV package body was not found");
        }
        return source.toString();
    }

    private static String withoutSuccessLog(String source) {
        String call = "log('set_session_user_direct',cwms_msg.msg_level_basic,l_msg);";
        int first = source.indexOf(call);
        int second = source.indexOf(call, first + call.length());
        if (first < 0 || second < 0 || source.indexOf(call, second + call.length()) >= 0
                || !source.substring(first, second).contains("when no_data_found")) {
            throw new IllegalStateException("Unexpected session logging implementation; refusing to patch");
        }
        return source.substring(0, first) + "null; -- successful pooled session setup is not a login"
                + source.substring(first + call.length());
    }

    private static void install(Connection owner, String source) throws SQLException {
        // Source comes exclusively from this disposable schema's USER_SOURCE.
        try (PreparedStatement ddl = owner.prepareStatement("create or replace " + source)) {
            ddl.execute();
        }
        try (PreparedStatement query = owner.prepareStatement(
                "select count(*) from user_errors where name = ? and type = ? and attribute = 'ERROR'")) {
            query.setString(1, "CWMS_ENV");
            query.setString(2, "PACKAGE BODY");
            try (ResultSet rows = query.executeQuery()) {
                rows.next();
                if (rows.getInt(1) != 0) {
                    throw new IllegalStateException("CWMS_ENV failed to compile");
                }
            }
        }
    }

    private static Map<String, Object> measure(CwmsDatabaseContainer<?> database, Connection observer,
                                              int variant, boolean logging, int sample) throws SQLException {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("variant", variant);
        result.put("logging", logging);
        result.put("sample", sample);
        try (Connection web = DriverManager.getConnection(database.getJdbcUrl(),
                CwmsDataApiSetupCallback.getWebUser(), database.getPassword())) {
            String sid;
            try (PreparedStatement query = web.prepareStatement("select sys_context('USERENV','SID') from dual");
                 ResultSet rows = query.executeQuery()) {
                rows.next();
                sid = rows.getString(1);
            }
            result.put("before", memory(observer, sid));
            List<Double> elapsed = new ArrayList<>();
            try (PreparedStatement setup = web.prepareStatement(
                    "begin cwms_env.set_session_user_direct(?, ?); end;")) {
                setup.setQueryTimeout(15);
                for (int call = 0; call < 20; call++) {
                    setup.setString(1, call % 2 == 0 ? database.getPdUser().toUpperCase()
                            : CwmsDataApiSetupCallback.getWebUser().toUpperCase());
                    setup.setString(2, call % 2 == 0 ? "HQ" : null);
                    long started = System.nanoTime();
                    setup.execute();
                    elapsed.add((System.nanoTime() - started) / 1e6);
                    if (call == 0) {
                        result.put("afterFirst", memory(observer, sid));
                    }
                    try (PreparedStatement query = web.prepareStatement(
                            "select sys_context('CWMS_ENV','CWMS_USER'), "
                                    + "sys_context('CWMS_ENV','CWMS_PRIVILEGE') from dual");
                         ResultSet context = query.executeQuery()) {
                        context.next();
                        String expected = call % 2 == 0 ? database.getPdUser().toUpperCase()
                                : CwmsDataApiSetupCallback.getWebUser().toUpperCase();
                        if (!expected.equals(context.getString(1))
                                || (call % 2 != 0 && !"READ_ONLY".equals(context.getString(2)))) {
                            throw new IllegalStateException("Session identity/privilege reset changed");
                        }
                    }
                }
            }
            result.put("callMs", elapsed);
            result.put("afterRepeated", memory(observer, sid));
        }
        return result;
    }

    private static Map<String, Long> memory(Connection observer, String sid) throws SQLException {
        try (PreparedStatement query = observer.prepareStatement(
                "select p.pga_used_mem, p.pga_alloc_mem, p.pga_max_mem from v$process p "
                        + "join v$session s on s.paddr = p.addr where s.sid = ?")) {
            query.setInt(1, Integer.parseInt(sid));
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) {
                    throw new IllegalStateException("Benchmark Oracle session disappeared");
                }
                Map<String, Long> result = new LinkedHashMap<>();
                result.put("usedBytes", rows.getLong(1));
                result.put("allocatedBytes", rows.getLong(2));
                result.put("maxBytes", rows.getLong(3));
                return result;
            }
        }
    }
}
