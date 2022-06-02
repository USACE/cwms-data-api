package cwms.radar.data.dao;

import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.Clob;
import cwms.radar.data.dto.Clobs;
import cwms.radar.data.dto.Office;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record4;
import org.jooq.RecordMapper;
import org.jooq.Select;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.SelectLimitPercentStep;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import usace.cwms.db.jooq.codegen.tables.AV_CLOB;
import usace.cwms.db.jooq.codegen.tables.AV_OFFICE;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.inline;

public class ClobDao extends JooqDao<Clob>
{
	private static Logger logger = Logger.getLogger(ClobDao.class.getName());

	public ClobDao(DSLContext dsl)
	{
		super(dsl);
	}

	// Yikes, I hate this method - it retrieves all the clobs?  That could be gigabytes of data.
	// Not returning Value or Desc fields until a useful way of working with this method is figured out.
	@Override
	public List<Clob> getAll(Optional<String> limitToOffice)
	{
		AV_CLOB ac = AV_CLOB.AV_CLOB;
		AV_OFFICE ao = AV_OFFICE.AV_OFFICE;

		SelectJoinStep<Record2<String, String>> joinStep = dsl.select(ac.ID, ao.OFFICE_ID).from(
				ac.join(ao).on(ac.OFFICE_CODE.eq(ao.OFFICE_CODE)));

		Select<Record2<String, String>> select = joinStep;

		if(limitToOffice.isPresent())
		{
			String office = limitToOffice.get();
			if(office != null && !office.isEmpty())
			{
				SelectConditionStep<Record2<String, String>> conditionStep = joinStep.where(ao.OFFICE_ID.eq(office));
				select = conditionStep;
			}
		}

		RecordMapper<Record2<String, String>, Clob> mapper = joinRecord ->
			new Clob(joinRecord.get(ao.OFFICE_ID),
					joinRecord.get(ac.ID),null, null);

		return select.fetch(mapper);
	}

	@Override
	public Optional<Clob> getByUniqueName(String uniqueName, Optional<String> limitToOffice)
	{
		AV_CLOB ac = AV_CLOB.AV_CLOB;
		AV_OFFICE ao = AV_OFFICE.AV_OFFICE;

		Condition cond = ac.ID.eq(uniqueName);
		if(limitToOffice.isPresent())
		{
			String office = limitToOffice.get();
			if(office != null && !office.isEmpty())
			{
				cond = cond.and(ao.OFFICE_ID.eq(office));
			}
		}

		RecordMapper<Record, Clob> mapper = joinRecord ->
				new Clob(joinRecord.getValue(ao.OFFICE_ID),
					joinRecord.getValue(ac.ID),
					joinRecord.getValue(ac.DESCRIPTION),
					joinRecord.getValue(ac.VALUE)
			);

		Clob avClob = dsl.select(ao.OFFICE_ID, ac.asterisk() ).from(
				ac.join(ao).on(ac.OFFICE_CODE.eq(ao.OFFICE_CODE))).where(cond).fetchOne(mapper);

		return Optional.ofNullable(avClob);
	}

	public Clobs getClobs(String cursor, int pageSize, Optional<String> office, boolean includeValues ){
		return getClobs(cursor,pageSize,office,includeValues,".*");
	}

	public Clobs getClobs(String cursor, int pageSize, Optional<String> office, boolean includeValues, String like ){
		int total = 0;
		String clobCursor = "*";
		AV_CLOB v_clob = AV_CLOB.AV_CLOB;
		AV_OFFICE v_office = AV_OFFICE.AV_OFFICE;

		if( cursor == null || cursor.isEmpty() ){

			SelectConditionStep<Record1<Integer>> count =
				dsl.select(count(asterisk()))
				   .from(v_clob)
				   .join(v_office).on(v_clob.OFFICE_CODE.eq(v_office.OFFICE_CODE))
				   .where(v_clob.ID.upper().likeRegex(like.toUpperCase()))
				   .and(v_office.OFFICE_ID.upper().like(office.isPresent() ? office.get().toUpperCase() : "%"));

			total = count.fetchOne().value1().intValue();
		} else {
			String[] parts = Catalog.decodeCursor(cursor, "||");

            logger.fine("decoded cursor: " + String.join("||", parts));
            for( String p: parts){
                logger.finest(p);
            }

            if(parts.length > 1) {
                clobCursor = parts[0].split(";")[0];
				clobCursor = clobCursor.substring(clobCursor.indexOf("/")+1); // ditch the officeId that's embedded in
                total = Integer.parseInt(parts[1]);
				pageSize = Integer.parseInt(parts[2]);
            }
		}
/*
		Table<?> forLimit = dsl.select(v_clob.ID,v_office.OFFICE_ID)
							   .from(v_clob)
							   .join(v_office).on(v_clob.OFFICE_CODE.eq(v_office.OFFICE_CODE))
							   .where(v_clob.ID.likeRegex(like))
							   .and(v_office.OFFICE_ID.like( office.isPresent() ? office.get() : "%"))
							   .and(v_clob.ID.upper().greaterThan(clobCursor))
							   .orderBy(v_clob.ID).limit(pageSize).asTable();*/


		SelectLimitPercentStep<Record4<String, String, String, String>> query = dsl.select(
												v_office.OFFICE_ID,
												v_clob.ID,
												v_clob.DESCRIPTION,
												includeValues == true ? v_clob.VALUE : DSL.inline("").as(v_clob.VALUE)
												)
									   .from(v_clob)
									   //.innerJoin(forLimit).on(forLimit.field(v_clob.ID).eq(v_clob.ID))
									   .join(v_office).on(v_clob.OFFICE_CODE.eq(v_office.OFFICE_CODE))
									   .where(v_clob.ID.upper().likeRegex(like.toUpperCase()))
									   .and(v_clob.ID.upper().greaterThan(clobCursor))
									   .orderBy(v_clob.ID).limit(pageSize);
									   ;

		Clobs.Builder builder = new Clobs.Builder(clobCursor,pageSize, total);
		logger.finest( () -> { return query.getSQL(ParamType.INLINED);});
		query.fetch().forEach( row -> {
			usace.cwms.db.jooq.codegen.tables.records.AV_CLOB clob = row.into(v_clob);
			usace.cwms.db.jooq.codegen.tables.records.AV_OFFICE clobOffice = row.into(v_office);
			builder.addClob( new Clob(
				clobOffice.getOFFICE_ID(),
				clob.getID(),
				clob.getDESCRIPTION(),
				clob.getVALUE()
			));

		});

		return builder.build();
	}


	public List<Clob> getClobsLike(String office, String idLike)
	{
		AV_CLOB ac = AV_CLOB.AV_CLOB;
		AV_OFFICE ao = AV_OFFICE.AV_OFFICE;

		Condition cond = ac.ID.upper().like(idLike.toUpperCase());
		if(office != null && !office.isEmpty())
		{
			cond = cond.and(ao.OFFICE_ID.upper().eq(office.toUpperCase()));
		}

		RecordMapper<Record, Clob> mapper = joinRecord ->
				new Clob(joinRecord.get(ao.OFFICE_ID),
						joinRecord.get(ac.ID),
						joinRecord.get(ac.DESCRIPTION),
						joinRecord.get(ac.VALUE)
				);

		return dsl.select(ac.asterisk(), ao.OFFICE_ID).from(
				ac.join(ao).on(ac.OFFICE_CODE.eq(ao.OFFICE_CODE))).where(cond).fetch(mapper);
	}

	public String getClobValue(String office, String id)
	{
		AV_CLOB ac = AV_CLOB.AV_CLOB;
		AV_OFFICE ao = AV_OFFICE.AV_OFFICE;

		Condition cond = ac.ID.eq(id).and(ao.OFFICE_ID.eq(office));

		Record1<String> clobRecord = dsl.select(ac.VALUE).from(
				ac.join(ao).on(ac.OFFICE_CODE.eq(ao.OFFICE_CODE))).where(cond).fetchOne();

		return clobRecord.value1();
	}

}
