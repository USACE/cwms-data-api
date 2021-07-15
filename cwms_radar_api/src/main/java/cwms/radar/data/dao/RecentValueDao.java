package cwms.radar.data.dao;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import cwms.radar.data.dto.Tsv;
import cwms.radar.data.dto.TsvDqu;
import cwms.radar.data.dto.TsvDquId;
import cwms.radar.data.dto.TsvId;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.SelectConditionStep;
import org.jooq.SelectHavingStep;

import usace.cwms.db.jooq.codegen.tables.AV_TSV;
import usace.cwms.db.jooq.codegen.tables.AV_TSV_DQU;

import static org.jooq.impl.DSL.asterisk;
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
			Long tsCode,
			String unit)
	{
		// For some timeseries it seems something was misconfigured and had inserted values from the far future.
		// If I remember right it was like year 10000.
		// It seems like a forecasted values in the next two weeks or so might be reasonable to return.
		GregorianCalendar gregorianCalendar = new GregorianCalendar();
		gregorianCalendar.add(GregorianCalendar.HOUR, 24 * 14);
		Timestamp twoWeeksFromNow = Timestamp.from(gregorianCalendar.toInstant());

		gregorianCalendar = new GregorianCalendar();
		gregorianCalendar.add(GregorianCalendar.HOUR, 24 * -14);
		Timestamp twoWeeksAgo = Timestamp.from(gregorianCalendar.toInstant());

		return findMostRecent(tOfficeId, tsCode, unit, twoWeeksFromNow, twoWeeksAgo);
	}






	public TsvDqu findMostRecent(String tOfficeId, Long tsCode, String unit, Timestamp twoWeeksFromNow, Timestamp twoWeeksAgo)
	{
		TsvDqu retval = null;

		AV_TSV_DQU view = AV_TSV_DQU.AV_TSV_DQU;

		Condition nestedCondition = view.ALIASED_ITEM.isNull()
						.and(view.VALUE.isNotNull())
			.and(view.TS_CODE.eq(tsCode))
			.and(view.OFFICE_ID.eq(tOfficeId));

		if(twoWeeksFromNow != null){
			nestedCondition = nestedCondition.and(view.DATE_TIME.lt(twoWeeksFromNow));
		}

		if(twoWeeksAgo != null){
			nestedCondition = nestedCondition.and(view.DATE_TIME.gt(twoWeeksAgo));
		}

		String maxFieldName = "MAX_DATE_TIME";
		SelectHavingStep<Record1<Timestamp>> select = dsl.select(max(view.DATE_TIME).as(maxFieldName)).from(
				view).where(nestedCondition).groupBy(view.TS_CODE);

		Record record = dsl.select(asterisk()).from(view).where(view.DATE_TIME.in(select)).and(
				view.TS_CODE.eq(tsCode)).and(view.OFFICE_ID.eq(tOfficeId)).and(view.UNIT_ID.eq(unit)).and(
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

	private Condition and(Condition condition, Condition subCondition)
	{
		if(condition == null)
		{
			condition = subCondition;
		}
		else
		{
			condition = condition.and(subCondition);
		}
		return condition;
	}

	// This is basically that was used for the sparklines...
	public List<Tsv> findInDateRange(Collection<Long> ts_codes, Date startDate, Date endDate) {
		List<Tsv> retval = Collections.emptyList();

		if (ts_codes != null && !ts_codes.isEmpty()) {

			Timestamp start = new Timestamp(startDate.getTime());
			Timestamp end = new Timestamp(endDate.getTime());

			AV_TSV view = AV_TSV.AV_TSV;
			retval = dsl.select(view.asterisk()).from(view).where(
					view.TS_CODE.in(ts_codes).and(view.DATE_TIME.ge(start)).and(view.DATE_TIME.lt(end)).and(
							view.START_DATE.le(end)).and(view.END_DATE.gt(start))).orderBy(view.DATE_TIME).fetch(
					jrecord -> {
						usace.cwms.db.jooq.codegen.tables.records.AV_TSV into = jrecord.into(view);

						TsvId id = new TsvId(into.getTS_CODE(), into.getDATE_TIME(), into.getVERSION_DATE(),
								into.getDATA_ENTRY_DATE());
						Tsv tsv = new Tsv(id, into.getVALUE(), into.getQUALITY_CODE(), into.getSTART_DATE(),
								into.getEND_DATE());

						return tsv;
					});
		}
		return retval;
	}

	public List<Tsv> findMostRecentsInRange(List<Long> tsCodes, Date thirtyDaysAgo, Date twoWeeksFromNow) {
		List<Tsv> retval = Collections.emptyList();

		if (tsCodes != null && !tsCodes.isEmpty()) {

					// // This is slow...
					//				"SELECT f.* "
					//				+ "FROM\n"
					//				+ "  (SELECT TS_CODE,\n"
					//				+ "    MAX(DATE_TIME) AS MAXDATE\n"
					//				+ "  FROM CWMS_20.AV_TSV\n"
					//				+ "  WHERE TS_CODE IN "
					//				+ "		(:tsCodes)\n"
					//				+ "   AND DATE_TIME < :futuredate \n"
					//				+ "   AND DATE_TIME > :pastdate \n"
					//				+ "  GROUP BY ts_code\n"
					//				+ "  )                       x\n"
					//				+ "INNER JOIN CWMS_20.AV_TSV f\n"
					//				+ "ON f.TS_CODE    = x.TS_CODE\n"
					//				+ "AND f.DATE_TIME = x.MAXDATE"


					//// Brian Baley suggested the below:
					//				"WITH cte_max_codes AS\n" +
					//				"  (SELECT ts_code,\n" +
					//				"    date_time,\n" +
					//				"    version_date,\n" +
					//				"    data_entry_date,\n" +
					//				"    value,\n" +
					//				"    quality_code,\n" +
					//				"    start_date,\n" +
					//				"    end_date,\n" +
					//				"    row_number() over ( partition BY ts_code order by DATE_TIME DESC ) AS row_num\n" +
					//				"  FROM cwms_v_tsv\n" +
					//				"  WHERE ts_code  IN (:tsCodes)\n" +
					//				"  AND DATE_TIME   < :futuredate\n" +
					//				"  AND DATE_TIME   > :pastdate\n" +
					//				"  AND start_date >= :pastdate - 366\n" +
					//				"  AND end_date   <= :futuredate + 366\n" +
					//				"  )\n" +
					//				"SELECT ts_code,\n" +
					//				"  date_time,\n" +
					//				"  version_date,\n" +
					//				"  data_entry_date,\n" +
					//				"  value,\n" +
					//				"  quality_code,\n" +
					//				"  start_date,\n" +
					//				"  end_date\n" +
					//				"FROM cte_max_codes\n" +
					//				"WHERE row_num = 1 "
					///// Seems the real improvement was from the +/- 366 conditions, going back to the max() version.

			//// Peter's suggestion
//			"SELECT ts_code, date_time, version_date, data_entry_date, value, quality_code, start_date, end_date "
//					+ " FROM "
//					+ "( "
//					+ "SELECT tsv.*, "
//					+ "       max(date_time) over (partition by ts_code) max_date_time "
//					+ "FROM  CWMS_20.AV_TSV tsv " // AV_TSV is actually a union across tsv tables per year
//					+ "WHERE "
//					+ "ts_code IN (:tsCodes) "
//					+ "AND DATE_TIME < :futuredate "
//					+ "AND DATE_TIME > :pastdate "
//					+ "  AND start_date <= :futuredate \n"
//					+ "  AND end_date   > :pastdate \n"
//					+ ") "
//					+ "WHERE date_time = max_date_time "
			Timestamp pastdate = new Timestamp(thirtyDaysAgo.getTime());
			Timestamp futuredate = new Timestamp(twoWeeksFromNow.getTime());
			AV_TSV view = AV_TSV.AV_TSV;
			SelectConditionStep<Record> inner_select = dsl.select(view.asterisk(),
					max(view.DATE_TIME).over(partitionBy(view.TS_CODE)).as("max_date_time"))
					.from(view)
					.where(
						view.DATE_TIME.lt(futuredate)
								.and(view.DATE_TIME.gt(pastdate))
								.and(view.START_DATE.le(futuredate))
								.and(view.END_DATE.gt(pastdate)));
			retval = dsl.select(view.asterisk()).from(inner_select)
					.where(view.DATE_TIME.eq((Field<Timestamp>) inner_select.field("max_date_time")))
			.fetch( jrecord -> {
						usace.cwms.db.jooq.codegen.tables.records.AV_TSV into = jrecord.into(view);

						TsvId id = new TsvId(into.getTS_CODE(), into.getDATE_TIME(), into.getVERSION_DATE(),
								into.getDATA_ENTRY_DATE());
						Tsv tsv = new Tsv(id, into.getVALUE(), into.getQUALITY_CODE(), into.getSTART_DATE(),
								into.getEND_DATE());

						return tsv;
					});
		}
		return retval;
	}


}
