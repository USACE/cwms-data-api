package cwms.radar.data.dao;

import java.util.logging.Logger;

import cwms.radar.data.dto.Blob;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.ResultQuery;

public class BlobDao extends JooqDao<Blob>
{
	private static Logger logger = Logger.getLogger(BlobDao.class.getName());

	public BlobDao(DSLContext dsl)
	{
		super(dsl);
	}

	public Blob getById(String id, String office)
	{
		ResultQuery<Record> query = dsl.resultQuery(
						"SELECT AT_BLOB.ID, AT_BLOB.DESCRIPTION, CWMS_MEDIA_TYPE.MEDIA_TYPE_ID, CWMS_OFFICE.OFFICE_ID," +
								"AT_BLOB.VALUE \n"
								+ "FROM CWMS_20.AT_BLOB \n"
								+ "join CWMS_20.CWMS_MEDIA_TYPE on AT_BLOB.MEDIA_TYPE_CODE = CWMS_MEDIA_TYPE.MEDIA_TYPE_CODE \n"
								+ "join CWMS_20.CWMS_OFFICE on AT_BLOB.OFFICE_CODE=CWMS_OFFICE.OFFICE_CODE \n"
				+ "WHERE OFFICE_ID = :officeId and ID = :id"
						);

		query.bind("officeId", office);
		query.bind("id", id);

		Blob blob = query.fetchOne(r -> {
			String rId = r.get("ID", String.class);
			String rOffice = r.get("OFFICE_ID", String.class);
			String rDesc = r.get("DESCRIPTION", String.class);
			String rMedia = r.get("MEDIA_TYPE_ID", String.class);
//			byte[] value = r.get("VALUE", byte[] );
			return new Blob(rOffice, rId, rDesc, rMedia, null);
		});

		return blob;
	}








}
