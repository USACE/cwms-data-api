package cwms.cda.data.dto.auth;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.data.dto.auth.users.User;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class UserTest {

    @Test
    void createUser_allFieldsProvided_success() {
        Map<String, List<String>> roles = new HashMap<>();
        roles.put("SWT", List.of("CWMS Users", "TS ID Creator"));

        List<TsGroupPrivilege> privileges = List.of(
            new TsGroupPrivilege(100, "policy-dam_operator-r-72h", "read", 72)
        );

        User user = new User("m5hectest", "issuer::m5hectest", "m5hectest@test.com",
            true, roles, privileges);

        assertAll(
            () -> assertEquals("m5hectest", user.getUserName(), "User name"),
            () -> assertEquals("issuer::m5hectest", user.getPrincipal(), "Principal"),
            () -> assertEquals("m5hectest@test.com", user.getEmail(), "Email"),
            () -> assertTrue(user.getCacAuth(), "CAC auth flag"),
            () -> assertNotNull(user.getRoles(), "Roles"),
            () -> assertEquals(2, user.getRoles().get("SWT").size(), "SWT roles count"),
            () -> assertNotNull(user.getTsGroupPrivileges(), "TS group privileges"),
            () -> assertEquals(1, user.getTsGroupPrivileges().size(), "TS group privileges count"),
            () -> assertEquals(72, user.getTsGroupPrivileges().get(0).getEmbargoHours(), "Embargo hours")
        );
    }

    @Test
    void createUser_serialize_roundtrip() {
        Map<String, List<String>> roles = new HashMap<>();
        roles.put("SWT", List.of("CWMS Users", "TS ID Creator"));
        roles.put("SPK", List.of("CWMS Users"));

        List<TsGroupPrivilege> privileges = List.of(
            new TsGroupPrivilege(100, "policy-dam_operator-r-72h", "read", 72),
            new TsGroupPrivilege(200, "policy-viewer_users-r-7d", "read-write", 168)
        );

        User original = new User("m5hectest", "issuer::m5hectest", "m5hectest@test.com",
            true, roles, privileges);

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, original);
        User deserialized = Formats.parseContent(contentType, json, User.class);

        assertAll(
            () -> assertEquals(original.getUserName(), deserialized.getUserName(), "User name"),
            () -> assertEquals(original.getPrincipal(), deserialized.getPrincipal(), "Principal"),
            () -> assertEquals(original.getEmail(), deserialized.getEmail(), "Email"),
            () -> assertNotNull(deserialized.getRoles(), "Roles"),
            () -> assertEquals(2, deserialized.getRoles().get("SWT").size(), "SWT roles"),
            () -> assertEquals(1, deserialized.getRoles().get("SPK").size(), "SPK roles"),
            () -> assertNotNull(deserialized.getTsGroupPrivileges(), "TS group privileges"),
            () -> assertEquals(2, deserialized.getTsGroupPrivileges().size(), "TS group privilege count"),
            () -> assertEquals(72, deserialized.getTsGroupPrivileges().get(0).getEmbargoHours(), "First embargo hours"),
            () -> assertEquals(168, deserialized.getTsGroupPrivileges().get(1).getEmbargoHours(), "Second embargo hours"),
            () -> assertEquals("read", deserialized.getTsGroupPrivileges().get(0).getPrivilege(), "First privilege"),
            () -> assertEquals("read-write", deserialized.getTsGroupPrivileges().get(1).getPrivilege(), "Second privilege")
        );
    }

    @Test
    void createUser_emptyTsGroupPrivileges_roundtrip() {
        User original = new User("testuser", "principal", "test@test.com",
            false, new HashMap<>(), Collections.emptyList());

        ContentType contentType = new ContentType(Formats.JSON);
        String json = Formats.format(contentType, original);
        User deserialized = Formats.parseContent(contentType, json, User.class);

        assertAll(
            () -> assertEquals(original.getUserName(), deserialized.getUserName(), "User name"),
            () -> assertNotNull(deserialized.getTsGroupPrivileges(), "TS group privileges"),
            () -> assertTrue(deserialized.getTsGroupPrivileges().isEmpty(), "TS group privileges should be empty")
        );
    }

    @Test
    void createUser_builder_success() {
        User.Builder builder = new User.Builder("testuser", "principal", "test@test.com", true);
        builder.addRole("SWT", "CWMS Users");
        builder.addRole("SWT", "TS ID Creator");
        builder.addRole("SPK", "CWMS Users");
        builder.addTsGroupPrivilege(new TsGroupPrivilege(100, "group-72h", "read", 72));
        builder.addTsGroupPrivilege(new TsGroupPrivilege(200, "group-7d", "write", 168));

        User user = builder.build();

        assertAll(
            () -> assertEquals("testuser", user.getUserName(), "User name"),
            () -> assertEquals(2, user.getRoles().get("SWT").size(), "SWT roles"),
            () -> assertEquals(1, user.getRoles().get("SPK").size(), "SPK roles"),
            () -> assertEquals(2, user.getTsGroupPrivileges().size(), "TS group privilege count"),
            () -> assertEquals("group-72h", user.getTsGroupPrivileges().get(0).getTsGroupId(), "First TS group ID"),
            () -> assertEquals("group-7d", user.getTsGroupPrivileges().get(1).getTsGroupId(), "Second TS group ID")
        );
    }

    @Test
    void createUser_builderAddTsGroupPrivilegesList() {
        User.Builder builder = new User.Builder("testuser", "principal", "test@test.com", false);
        List<TsGroupPrivilege> privileges = List.of(
            new TsGroupPrivilege(100, "group-72h", "read", 72),
            new TsGroupPrivilege(200, "group-7d", "read-write", 168)
        );
        builder.addTsGroupPrivileges(privileges);

        User user = builder.build();
        assertEquals(2, user.getTsGroupPrivileges().size(), "TS group privilege count");
    }

    @Test
    void createUser_builderAddNullTsGroupPrivileges() {
        User.Builder builder = new User.Builder("testuser", "principal", "test@test.com", false);
        builder.addTsGroupPrivileges(null);

        User user = builder.build();
        assertAll(
            () -> assertNotNull(user.getTsGroupPrivileges(), "TS group privileges should not be null"),
            () -> assertTrue(user.getTsGroupPrivileges().isEmpty(), "TS group privileges should be empty")
        );
    }
}
