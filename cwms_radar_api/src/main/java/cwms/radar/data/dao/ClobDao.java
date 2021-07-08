package cwms.radar.data.dao;

import java.util.List;
import java.util.Optional;

import cwms.radar.data.dto.Clob;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.RecordMapper;
import org.jooq.Select;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;

import usace.cwms.db.jooq.codegen.tables.AV_CLOB;
import usace.cwms.db.jooq.codegen.tables.AV_OFFICE;

public class ClobDao extends JooqDao<Clob>
{
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


	public List<Clob> getClobsLike(String office, String idLike)
	{
		AV_CLOB ac = AV_CLOB.AV_CLOB;
		AV_OFFICE ao = AV_OFFICE.AV_OFFICE;

		Condition cond = ac.ID.like(idLike);
		if(office != null && !office.isEmpty())
		{
			cond = cond.and(ao.OFFICE_ID.eq(office));
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
