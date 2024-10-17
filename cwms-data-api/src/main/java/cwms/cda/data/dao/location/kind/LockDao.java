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

package cwms.cda.data.dao.location.kind;

import static java.util.stream.Collectors.toList;

import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.location.kind.Lock;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOCK_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_LOCK;
import usace.cwms.db.jooq.codegen.udt.records.LOCK_OBJ_T;

public final class LockDao extends JooqDao<Lock> {
    private static final AV_LOCK view = AV_LOCK.AV_LOCK;

    public LockDao(DSLContext dsl) {
        super(dsl);
    }

    public List<CwmsId> retrieveLockIds(CwmsId projectId, UnitSystem units) {
        if (units == null) {
            units = UnitSystem.SI;
        }
        final UnitSystem unitSystemFinal = units;
        return connectionResult(dsl, c -> {
            setOffice(c, projectId.getOfficeId());
            Result<Record> records = DSL.using(c)
                .select(view.fields())
                .from(view)
                .where(view.PROJECT_ID.eq(projectId.getName()))
                    .and(view.UNIT_SYSTEM.eq(unitSystemFinal.name()))
                .fetch();
            return records.stream().map(LockDao::map).collect(toList());
        });
    }

    public Lock retrieveLock(CwmsId lockId, UnitSystem units) {
        if (units == null) {
            units = UnitSystem.SI;
        }
        UnitSystem unitSystemFinal = units;
        return connectionResult(dsl, c -> {
            setOffice(c, lockId.getOfficeId());
            Record dbRecord = DSL.using(c)
                .select(view.fields())
                .from(view)
                .where(view.LOCK_ID.eq(lockId.getName()).and(view.DB_OFFICE_ID.eq(lockId.getOfficeId()))
                        .and(view.UNIT_SYSTEM.equalIgnoreCase(unitSystemFinal.name())))
                .fetchOne();
            if (dbRecord == null) {
                throw new NotFoundException("Lock not found: " + lockId);
            }
            return map(dbRecord.into(LOCK_OBJ_T.class));
        });

    }

    public void storeLock(Lock lock, boolean failIfExists) {
        connection(dsl, c -> {
            setOffice(c, lock.getLocation().getOfficeId());
            CWMS_LOCK_PACKAGE.call_STORE_LOCK(DSL.using(c).configuration(), map(lock), formatBool(failIfExists));
        });
    }

    static CwmsId map(Record r) {
        String officeId = r.getValue("DB_OFFICE_ID", String.class);
        String baseLocationId = r.getValue("PROJECT_ID", String.class);
        String subLocationId = r.getValue("LOCK_ID", String.class);
        return CwmsId.buildCwmsId(officeId, baseLocationId + "-" + subLocationId);
    }

    static LOCK_OBJ_T map(Lock lock) {
        LOCK_OBJ_T retval = new LOCK_OBJ_T();
        retval.setLOCK_LOCATION(LocationUtil.getLocation(lock.getLocation()));
        retval.setPROJECT_LOCATION_REF(LocationUtil.getLocationRef(lock.getProjectId()));
        retval.setLOCK_WIDTH(lock.getLockWidth());
        retval.setLOCK_LENGTH(lock.getLockLength());
        retval.setNORMAL_LOCK_LIFT(lock.getNormalLockLift());
        retval.setVOLUME_PER_LOCKAGE(lock.getVolumePerLockage());
        retval.setMINIMUM_DRAFT(lock.getMinimumDraft());
        retval.setUNITS_ID(lock.getUnits());
        retval.setVOLUME_UNITS_ID(lock.getVolumeUnits());
        return retval;
    }

    static Lock map(LOCK_OBJ_T lock) {
        return new Lock.Builder()
            .withLocation(LocationUtil.getLocation(lock.getLOCK_LOCATION()))
            .withProjectId(LocationUtil.getLocationIdentifier(lock.getPROJECT_LOCATION_REF()))
            .withLockLength(lock.getLOCK_LENGTH())
            .withLockWidth(lock.getLOCK_WIDTH())
            .withNormalLockLift(lock.getNORMAL_LOCK_LIFT())
            .withVolumePerLockage(lock.getVOLUME_PER_LOCKAGE())
            .withMinimumDraft(lock.getMINIMUM_DRAFT())
            .withUnits(lock.getUNITS_ID())
            .withVolumeUnits(lock.getVOLUME_UNITS_ID())
            .withVolumeUnits(lock.getVOLUME_UNITS_ID())
            .build();
    }

    public void deleteLock(CwmsId lockId, DeleteRule deleteRule) {
        connection(dsl, c -> {
            setOffice(c, lockId.getOfficeId());
            CWMS_LOCK_PACKAGE.call_DELETE_LOCK(DSL.using(c).configuration(), lockId.getName(), deleteRule.getRule(),
                lockId.getOfficeId());
        });
    }

    public void renameLock(CwmsId lockId, String newName) {
        connection(dsl, c -> {
            setOffice(c, lockId.getOfficeId());
            CWMS_LOCK_PACKAGE.call_RENAME_LOCK(DSL.using(c).configuration(), lockId.getName(), newName,
                lockId.getOfficeId());
        });
    }
}
