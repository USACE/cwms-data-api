package cwms.radar.data.dto;

import java.util.Date;
import javax.persistence.EmbeddedId;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

public class TsvDqu
{
	private TsvDquId id;
	private String cwmsTsId;
	private Date versionDate;
	private Date dataEntryDate;
	private Double value;
	private Long qualityCode;
	private Date startDate;
	private Date endDate;


	public TsvDqu() {
	}

	public TsvDqu(TsvDquId id) {
		this.id = id;
	}

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

	@EmbeddedId
	public TsvDquId getId() {
		return this.id;
	}

	public void setId(TsvDquId id) {
		this.id = id;
	}


	public String getCwmsTsId() {
		return this.cwmsTsId;
	}

	public void setCwmsTsId(String cwmsTsId) {
		this.cwmsTsId = cwmsTsId;
	}

	@Temporal(TemporalType.DATE)
	public Date getVersionDate() {
		return this.versionDate;
	}

	public void setVersionDate(Date versionDate) {
		this.versionDate = versionDate;
	}

	@Temporal(TemporalType.DATE)
	public Date getDataEntryDate() {
		return this.dataEntryDate;
	}

	public void setDataEntryDate(Date dataEntryDate) {
		this.dataEntryDate = dataEntryDate;
	}

	public Double getValue() {
		return this.value;
	}

	public void setValue(Double value) {
		this.value = value;
	}


	public Long getQualityCode() {
		return this.qualityCode;
	}

	public void setQualityCode(Long qualityCode) {
		this.qualityCode = qualityCode;
	}

	@Temporal(TemporalType.DATE)
	public Date getStartDate() {
		return this.startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	@Temporal(TemporalType.DATE)
	public Date getEndDate() {
		return this.endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}


	@Override
	public String toString()
	{
		return "TsvDqu{" + "id=" + id + ", cwmsTsId='" + cwmsTsId + '\'' + ", versionDate=" + versionDate + ", dataEntryDate=" + dataEntryDate + ", value=" + value + ", qualityCode=" + qualityCode + ", startDate=" + startDate + ", endDate=" + endDate + '}';
	}
}
