package cwms.radar.data.dto;

import java.util.Date;
import javax.persistence.EmbeddedId;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

public class Tsv implements java.io.Serializable {

	private TsvId id;

	private Double value;
	private Long qualityCode;
	private Date startDate;
	private Date endDate;

	public Tsv() {
	}

	public Tsv(TsvId id) {
		this.id = id;
	}

	public Tsv(TsvId id,
				 Double value, Long qualityCode, Date startDate,
				 Date endDate
	) {
		this.id = id;

		this.value = value;
		this.qualityCode = qualityCode;
		this.startDate = startDate;
		this.endDate = endDate;

	}

	@EmbeddedId
	public TsvId getId() {
		return this.id;
	}

	public void setId(TsvId id) {
		this.id = id;
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
		return "Tsv{" + "id=" + id + ", value=" + value + ", qualityCode=" + qualityCode + ", startDate=" + startDate + ", endDate=" + endDate + '}';
	}
}


