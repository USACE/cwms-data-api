package cwms.radar.data.dao;

import java.util.List;
import java.util.Optional;

import cwms.radar.data.dto.Blob;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;

public class BlobDao extends JooqDao<Blob>
{
	public BlobDao(DSLContext dsl)
	{
		super(dsl);
	}

	@Override
	public Optional<Blob> getByUniqueName(String id, Optional<String> limitToOffice)
	{
		String queryStr = "SELECT AT_BLOB.ID, AT_BLOB.DESCRIPTION, CWMS_MEDIA_TYPE.MEDIA_TYPE_ID, CWMS_OFFICE.OFFICE_ID, AT_BLOB.VALUE \n"
				+ "FROM CWMS_20.AT_BLOB \n"
				+ "join CWMS_20.CWMS_MEDIA_TYPE on AT_BLOB.MEDIA_TYPE_CODE = CWMS_MEDIA_TYPE.MEDIA_TYPE_CODE \n"
				+ "join CWMS_20.CWMS_OFFICE on AT_BLOB.OFFICE_CODE=CWMS_OFFICE.OFFICE_CODE \n"
				+ "WHERE ID = ?";
		ResultQuery<Record> query;
		if(limitToOffice.isPresent())
		{
			queryStr = queryStr + " and CWMS_OFFICE.OFFICE_ID = ?";
			query = dsl.resultQuery(queryStr, id, limitToOffice.get());
		} else {
			query = dsl.resultQuery(queryStr, id);
		}

		Blob retval = query.fetchOne(r -> {
			String rId = r.get("ID", String.class);
			String rOffice = r.get("OFFICE_ID", String.class);
			String rDesc = r.get("DESCRIPTION", String.class);
			String rMedia = r.get("MEDIA_TYPE_ID", String.class);
			byte[] value = r.get("VALUE", byte[].class);
			return new Blob(rOffice, rId, rDesc, rMedia, value);
		});

		return Optional.ofNullable(retval);
	}


	@Override
	public List<Blob> getAll(Optional<String> limitToOffice)
	{
		String queryStr = "SELECT AT_BLOB.ID, AT_BLOB.DESCRIPTION, CWMS_MEDIA_TYPE.MEDIA_TYPE_ID, CWMS_OFFICE.OFFICE_ID\n"
				+ " FROM CWMS_20.AT_BLOB \n" +
				"join CWMS_20.CWMS_MEDIA_TYPE on AT_BLOB.MEDIA_TYPE_CODE = CWMS_MEDIA_TYPE.MEDIA_TYPE_CODE \n"
				+ "join CWMS_20.CWMS_OFFICE on AT_BLOB.OFFICE_CODE=CWMS_OFFICE.OFFICE_CODE \n"
				;

		ResultQuery<Record> query;
		if(limitToOffice.isPresent())
		{
			queryStr = queryStr + " and upper(CWMS_OFFICE.OFFICE_ID) = upper(?)";
			query = dsl.resultQuery(queryStr, limitToOffice.get());
		} else {
			query = dsl.resultQuery(queryStr);
		}

		return query.fetch(r -> {
			String rId = r.get("ID", String.class);
			String rOffice = r.get("OFFICE_ID", String.class);
			String rDesc = r.get("DESCRIPTION", String.class);
			String rMedia = r.get("MEDIA_TYPE_ID", String.class);

			return new Blob(rOffice, rId, rDesc, rMedia, null);
		});
	}

	public List<Blob> getAll(Optional<String> limitToOffice, String like)
	{
		String queryStr = "SELECT AT_BLOB.ID, AT_BLOB.DESCRIPTION, CWMS_MEDIA_TYPE.MEDIA_TYPE_ID, CWMS_OFFICE.OFFICE_ID\n"
				+ " FROM CWMS_20.AT_BLOB \n" +
				"join CWMS_20.CWMS_MEDIA_TYPE on AT_BLOB.MEDIA_TYPE_CODE = CWMS_MEDIA_TYPE.MEDIA_TYPE_CODE \n"
				+ "join CWMS_20.CWMS_OFFICE on AT_BLOB.OFFICE_CODE=CWMS_OFFICE.OFFICE_CODE \n"
				+ " where REGEXP_LIKE (upper(AT_BLOB.ID), upper(?))"
				;


		ResultQuery<Record> query;
		if(limitToOffice.isPresent())
		{
			queryStr = queryStr + " and upper(CWMS_OFFICE.OFFICE_ID) = upper(?)";
			query = dsl.resultQuery(queryStr, like, limitToOffice.get());
		} else {
			query = dsl.resultQuery(queryStr, like);
		}

		return query.fetch(r -> {
			String rId = r.get("ID", String.class);
			String rOffice = r.get("OFFICE_ID", String.class);
			String rDesc = r.get("DESCRIPTION", String.class);
			String rMedia = r.get("MEDIA_TYPE_ID", String.class);

			return new Blob(rOffice, rId, rDesc, rMedia, null);
		});
	}

}
