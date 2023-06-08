package cwms.radar.data.dto;

import java.util.Date;

public class Tsv implements java.io.Serializable {

	private final TsvId id;
	private final Double value;
	private final Long qualityCode;
	private final Date startDate;
	private final Date endDate;


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

	public TsvId getId() {
		return this.id;
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
		return "Tsv{" + "id=" + id + ", value=" + value + ", qualityCode=" + qualityCode + ", startDate=" + startDate + ", endDate=" + endDate + '}';
	}
}


