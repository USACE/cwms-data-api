package cwms.radar.data.dao;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.stream.Collectors;

import cwms.radar.data.dto.Tsv;
import cwms.radar.data.dto.TsvDqu;
import cwms.radar.data.dto.TsvDquId;
import cwms.radar.data.dto.TsvId;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.ResultQuery;
import org.jooq.SelectConditionStep;
import org.jooq.SelectHavingStep;

import usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2;
import usace.cwms.db.jooq.codegen.tables.AV_TSV;
import usace.cwms.db.jooq.codegen.tables.AV_TSV_DQU;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.partitionBy;

public class RecentValueDao extends JooqDao<TsvDqu>
{
	protected RecentValueDao(DSLContext dsl)
	{
		super(dsl);
	}

	TsvDqu findMostRecent(
			String tOfficeId,
			String tsId,
			String unit)
	{
		GregorianCalendar gregorianCalendar = new GregorianCalendar();
		gregorianCalendar.add(GregorianCalendar.HOUR, 24 * 14);
		Timestamp twoWeeksFromNow = Timestamp.from(gregorianCalendar.toInstant());

		gregorianCalendar = new GregorianCalendar();
		gregorianCalendar.add(GregorianCalendar.HOUR, 24 * -14);
		Timestamp twoWeeksAgo = Timestamp.from(gregorianCalendar.toInstant());

		return findMostRecent(tOfficeId, tsId, unit, twoWeeksFromNow, twoWeeksAgo);
	}


	// Finds the single most recent TsvDqu within the time window.
	public TsvDqu findMostRecent(String tOfficeId, String tsId, String unit, Timestamp twoWeeksFromNow, Timestamp twoWeeksAgo)
	{
		TsvDqu retval = null;

		AV_TSV_DQU view = AV_TSV_DQU.AV_TSV_DQU;

		Condition nestedCondition = view.ALIASED_ITEM.isNull()
						.and(view.VALUE.isNotNull())
			.and(view.CWMS_TS_ID.eq(tsId))
			.and(view.OFFICE_ID.eq(tOfficeId));

		if(twoWeeksFromNow != null){
			nestedCondition = nestedCondition.and(view.DATE_TIME.lt(twoWeeksFromNow));
		}

		// Is this really optional?
		if(twoWeeksAgo != null){
			nestedCondition = nestedCondition.and(view.DATE_TIME.gt(twoWeeksAgo));
		}

		String maxFieldName = "MAX_DATE_TIME";
		SelectHavingStep<Record1<Timestamp>> select = dsl.select(max(view.DATE_TIME).as(maxFieldName)).from(
				view).where(nestedCondition).groupBy(view.TS_CODE);

		Record record = dsl.select(asterisk()).from(view).where(view.DATE_TIME.in(select)).and(
				view.CWMS_TS_ID.eq(tsId)).and(view.OFFICE_ID.eq(tOfficeId)).and(view.UNIT_ID.eq(unit)).and(
				view.VALUE.isNotNull()).and(view.ALIASED_ITEM.isNull()).fetchOne();

		if(record != null)
		{
			retval = record.map(r -> {
				usace.cwms.db.jooq.codegen.tables.records.AV_TSV_DQU dqu = r.into(view);
				TsvDqu tsv = null;
				if(r != null)
				{
					TsvDquId id = new TsvDquId(dqu.getOFFICE_ID(), dqu.getTS_CODE(), dqu.getUNIT_ID(), dqu.getDATE_TIME());
					tsv = new TsvDqu(id, dqu.getCWMS_TS_ID(), dqu.getVERSION_DATE(), dqu.getDATA_ENTRY_DATE(), dqu.getVALUE(), dqu.getQUALITY_CODE(), dqu.getSTART_DATE(), dqu.getEND_DATE());
				}
				return tsv;
			});
		}

		return retval;
	}


	// This is similar to the code used for sparklines...
	// Finds all the Tsv data points in the time range for all the specified tsIds.
	public List<Tsv> findInDateRange(Collection<String> tsIds, Date startDate, Date endDate) {
		List<Tsv> retval = Collections.emptyList();

		if (tsIds != null && !tsIds.isEmpty()) {

			Timestamp start = new Timestamp(startDate.getTime());
			Timestamp end = new Timestamp(endDate.getTime());

			AV_TSV tsvView = AV_TSV.AV_TSV;
			AV_CWMS_TS_ID2 tsView = AV_CWMS_TS_ID2.AV_CWMS_TS_ID2;
			retval = dsl.select(tsvView.asterisk(), tsView.CWMS_TS_ID)
					.from(tsvView.join(tsView).on(tsvView.TS_CODE.eq(tsView.TS_CODE.cast(Long.class))))
					.where(
					tsView.CWMS_TS_ID.in(tsIds).and(tsvView.DATE_TIME.ge(start)).and(tsvView.DATE_TIME.lt(end)).and(
							tsvView.START_DATE.le(end)).and(tsvView.END_DATE.gt(start))).orderBy(tsvView.DATE_TIME).fetch(
					jrecord -> buildTsvFromViewRow(jrecord.into(tsvView)));
		}
		return retval;
	}

	@NotNull
	private Tsv buildTsvFromViewRow(usace.cwms.db.jooq.codegen.tables.records.AV_TSV into)
	{
		TsvId id = new TsvId(into.getTS_CODE(), into.getDATE_TIME(), into.getVERSION_DATE(), into.getDATA_ENTRY_DATE());
		Tsv tsv = new Tsv(id, into.getVALUE(), into.getQUALITY_CODE(), into.getSTART_DATE(), into.getEND_DATE());

		return tsv;
	}

	// Finds single most recent value within the window for each of the tsCodes
	public List<Tsv> findMostRecentsInRange(List<String> tsIds,  Timestamp futuredate, Timestamp pastdate) {
		List<Tsv> retval = Collections.emptyList();

		if (tsIds != null && !tsIds.isEmpty()) {
			AV_TSV tsvView = AV_TSV.AV_TSV;
			AV_CWMS_TS_ID2 tsView = AV_CWMS_TS_ID2.AV_CWMS_TS_ID2;
			SelectConditionStep<Record> inner_select
					= dsl.select(tsvView.asterisk(),
					max(tsvView.DATE_TIME).over(partitionBy(tsvView.TS_CODE)).as("max_date_time"), tsView.CWMS_TS_ID)
					.from(tsvView.join(tsView).on(tsvView.TS_CODE.eq(tsView.TS_CODE.cast(Long.class))))
					.where(
							tsView.CWMS_TS_ID.in(tsIds)
									.and(tsvView.VALUE.isNotNull())
								.and(tsvView.DATE_TIME.lt(futuredate))
								.and(tsvView.DATE_TIME.gt(pastdate))
								.and(tsvView.START_DATE.le(futuredate))
								.and(tsvView.END_DATE.gt(pastdate)));


			String[] columns = new String[]{"ts_code","date_time","version_date","data_entry_date","value",
					"quality_code","start_date","end_date"};
			List<Field<Object>> fields = Arrays.stream(columns).map(c -> field(c)).collect(Collectors.toList());

			// I want to select tsvView.asterisk but we are selecting from an inner select and
			// even though the inner select selects tsvView.asterisk it isn't the same.
			// So we will just select the fields we want.  Unfortunately that means our results
			// won't map into AV_TSV.AV_TSV
			retval = dsl.select(fields)
					.from(inner_select)
					.where(field("DATE_TIME").eq(inner_select.field("max_date_time")))

			.fetch( jrecord -> {
				Timestamp dataEntryDate;
				// !!! skipping DATA_ENTRY_DATE for now.  Need to figure out how to fix mapping in jooq.
				//	!! dataEntryDate= jrecord.getValue("data_entry_date", Timestamp.class); // maps to oracle.sql.TIMESTAMP
				// !!!
				dataEntryDate = null;
				// !!!
				TsvId id = new TsvId(jrecord.getValue("ts_code", Long.class),
						jrecord.getValue("date_time", Timestamp.class),
						jrecord.getValue("version_date", Timestamp.class), dataEntryDate); // oracle timestamp?
				Tsv tsv = new Tsv(id, jrecord.getValue("value", Double.class),
						jrecord.getValue("quality_code", Long.class),
						jrecord.getValue("start_date", Timestamp.class),
						jrecord.getValue("end_date", Timestamp.class));

				return tsv;
			});
		}
		return retval;
	}

//	// I want some sort of method to build retrieve based on groups.
//	// This is what cwms mobile did but it assumes that there is
//	// a TS Category for each Location Group
//	// and that the TS Group is stored in the location group assignment alias
//	// thats sort of alot of assumptions.
//	public List<Object> findMostRecentTVUA( String officeId, String locCatId, String locGroupId,
//											Date twoWeeksFromNow, Date thirtyDaysAgo)
//	{
//		List<Object> retval = null;
//
//		ResultQuery<Record> query = dsl.resultQuery(
//		"SELECT tsCode, dateValue, value, tsGroupId, baseParameterId, parameterId, unitId, tsGroupAliasId \n"
//				+ "FROM \n"
//				+ "  (SELECT "
//				+ "tsv.ts_code tsCode, "
//				+ "tsv.date_time dateValue, "
//				+ "tsv.value, "
//				+ "loc_ts_join.GROUP_ID tsGroupId, "
//				+ "cwms_ts_id.BASE_PARAMETER_ID baseParameterId, "
//				+ "cwms_ts_id.PARAMETER_ID parameterId, "
//				+ "cwms_ts_id.unit_id unitId, "
//				+ "loc_ts_join.alias_id tsGroupAliasId, \n"
//				+ "    MAX(tsv.date_time) over (partition BY tsv.ts_code) max_date_time \n"
//				+ "  FROM \n"
//				+ "    (SELECT tsGrp.*, \n"
//				+ "      MIN(tsGrp.attribute) over (partition BY tsGrp.GROUP_ID) min_attr, \n"
//				+ "		 locGrp.attribute loc_attr \n"
//				+ "    FROM CWMS_20.AV_LOC_GRP_ASSGN locGrp \n"
//				+ "    INNER JOIN CWMS_20.AV_TS_GRP_ASSGN tsGrp \n"
//				+ "    ON locGrp.GROUP_ID      = tsGrp.CATEGORY_ID \n"
//				+ "    AND locGrp.ALIAS_ID     = tsGrp.GROUP_ID \n"
//				+ "    AND locGrp.category_id  = :locCatId \n"
//				+ "    AND locGrp.GROUP_ID     = :locGroupId \n"
//				+ "    AND locGrp.DB_OFFICE_ID = :officeId \n"
//				+ "    ORDER BY loc_attr \n"
//				+ "    ) loc_ts_join \n"
//				+ "  INNER JOIN CWMS_20.av_tsv tsv \n"
//				+ "  CROSS JOIN CWMS_20.AV_CWMS_TS_ID cwms_ts_id \n"
//				+ "  ON tsv.ts_code          = loc_ts_join.ts_code \n"
//				+ "  AND cwms_ts_id.ts_code  = loc_ts_join.ts_code \n"
//				+ "  WHERE attribute = min_attr \n"
//				+ "  AND DATE_TIME < :futuredate \n"
//				+ "  AND DATE_TIME > :pastdate \n"
//				+ "  AND start_date <= :futuredate \n"
//				+ "  AND end_date   > :pastdate \n"
//				+ "  ) a \n"
//				+ "WHERE dateValue = max_date_time "
//		);
//		query.bind("futuredate", twoWeeksFromNow);
//		query.bind("pastdate", thirtyDaysAgo);
//		query.bind("officeId", officeId);
//		query.bind("locCatId", locCatId);
//		query.bind("locGroupId", locGroupId);
//
//
//		Result<Record> records = query.fetch();
//
//		retval = records.map(r -> {
//			System.out.println("What is this?");
//			return null;
//		});
//
//		return retval;
//	}


}
