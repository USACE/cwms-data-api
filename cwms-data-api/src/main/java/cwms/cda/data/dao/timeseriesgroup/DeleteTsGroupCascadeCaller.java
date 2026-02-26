package cwms.cda.data.dao.timeseriesgroup;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Optional;
import org.jooq.Configuration;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;

// Is this worth it?  Is there any possible reason we'd want to call the codegen'd version rather than our own?
public final class DeleteTsGroupCascadeCaller {
    private DeleteTsGroupCascadeCaller() {
    }

    // Cache lookup once.
    private static final Optional<MethodHandle> GENERATED_CALL = findGeneratedCall();

    private static Optional<MethodHandle> findGeneratedCall() {
        try {

            // Has to match exactly: public static void call_DELETE_TS_GROUP_CASCADE(
            //      Configuration configuration, String P_TS_CATEGORY_ID, String P_TS_GROUP_ID, String P_CASCADE, String P_DB_OFFICE_ID)
            MethodType mt = MethodType.methodType(
                    void.class,
                    org.jooq.Configuration.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class
            );

            MethodHandle mh = MethodHandles.publicLookup().findStatic(
                    CWMS_TS_PACKAGE.class,
                    "call_DELETE_TS_GROUP_CASCADE",
                    mt
            );

            return Optional.of(mh);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            return Optional.empty(); // old codegen: method not present
        }
    }

    public static boolean isGeneratedAvailable() {
        return GENERATED_CALL.isPresent();
    }

    public static void call_DELETE_TS_GROUP_CASCADE(
            org.jooq.Configuration cfg,
            String tsCategoryId,
            String tsGroupId,
            String cascade,
            String dbOfficeId
    ) throws Throwable {
        if (isGeneratedAvailable()) {
            GENERATED_CALL.get().invokeExact(cfg, tsCategoryId, tsGroupId, cascade, dbOfficeId);
            return;
        }

        // Fallback to local implementation:
        local_DELETE_TS_GROUP_CASCADE(cfg, tsCategoryId, tsGroupId, cascade, dbOfficeId);
    }

    public static void local_DELETE_TS_GROUP_CASCADE(Configuration configuration, String P_TS_CATEGORY_ID, String P_TS_GROUP_ID, String P_CASCADE, String P_DB_OFFICE_ID) {
        DELETE_TS_GROUP_CASCADE p = new DELETE_TS_GROUP_CASCADE();
        p.setP_TS_CATEGORY_ID(P_TS_CATEGORY_ID);
        p.setP_TS_GROUP_ID(P_TS_GROUP_ID);
        p.setP_CASCADE(P_CASCADE);
        p.setP_DB_OFFICE_ID(P_DB_OFFICE_ID);
        p.execute(configuration);
    }

}