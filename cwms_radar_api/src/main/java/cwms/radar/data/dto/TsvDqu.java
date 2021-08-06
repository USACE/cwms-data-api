package cwms.radar.data.dto;

import java.util.Date;

public class TsvDqu
{
	private final TsvDquId id;
	private final String cwmsTsId;
	private final Date versionDate;
	private final Date dataEntryDate;
	private final Double value;
	private final Long qualityCode;
	private final Date startDate;
	private final Date endDate;


	public TsvDqu(TsvDquId id, String cwmsTsId, Date versionDate,
				  Date dataEntryDate, Double value, Long qualityCode, Date startDate,
				  Date endDate	) {
		this.id = id;
		this.cwmsTsId = cwmsTsId;
		this.versionDate = versionDate;
		this.dataEntryDate = dataEntryDate;
		this.value = value;
		this.qualityCode = qualityCode;
		this.startDate = startDate;
		this.endDate = endDate;
	}

	public TsvDquId getId() {
		return this.id;
	}

	public String getCwmsTsId() {
		return this.cwmsTsId;
	}

	public Date getVersionDate() {
		return this.versionDate;
	}

	public Date getDataEntryDate() {
		return this.dataEntryDate;
	}

	public Double getValue() {
		return this.value;
	}

	public Long getQualityCode() {
		return this.qualityCode;
	}

	public Date getStartDate() {
		return this.startDate;
	}

	public Date getEndDate() {
		return this.endDate;
	}

	@Override
	public String toString()
	{
		return "TsvDqu{" + "id=" + id + ", cwmsTsId='" + cwmsTsId + '\'' + ", versionDate=" + versionDate + ", dataEntryDate=" + dataEntryDate + ", value=" + value + ", qualityCode=" + qualityCode + ", startDate=" + startDate + ", endDate=" + endDate + '}';
	}
}
