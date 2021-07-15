package cwms.radar.data.dto;

import java.util.Date;
import java.util.Objects;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

public class TsvId implements java.io.Serializable {

	private Long tsCode;

	private Date dateTime;

	private Date versionDate;

	private Date dataEntryDate;

	public TsvId() {
	}

	public TsvId(Long tsCode, Date dateTime, Date versionDate,
				 Date dataEntryDate) {

		this.tsCode = tsCode;

		this.dateTime = dateTime;
		this.versionDate = versionDate;
		this.dataEntryDate = dataEntryDate;
	}


	public Long getTsCode() {
		return this.tsCode;
	}

	public void setTsCode(Long tsCode) {
		this.tsCode = tsCode;
	}


	@Temporal(javax.persistence.TemporalType.TIMESTAMP)
	public Date getDateTime() {
		return this.dateTime;
	}

	public void setDateTime(Date dateTime) {
		this.dateTime = dateTime;
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

	@Override
	public int hashCode() {
		int hash = 7;

		hash = 41 * hash + Objects.hashCode(this.tsCode);
		hash = 41 * hash + Objects.hashCode(this.dateTime);
		hash = 41 * hash + Objects.hashCode(this.versionDate);
		hash = 41 * hash + Objects.hashCode(this.dataEntryDate);
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof TsvId)) {
			return false;
		}
		final TsvId other = (TsvId) obj;

		if (!Objects.equals(this.tsCode, other.tsCode)) {
			return false;
		}

		if (!Objects.equals(this.dateTime, other.dateTime)) {
			return false;
		}
		if (!Objects.equals(this.versionDate, other.versionDate)) {
			return false;
		}
		if (!Objects.equals(this.dataEntryDate, other.dataEntryDate)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString()
	{
		return "TsvId{" + "tsCode=" + tsCode + ", dateTime=" + dateTime + ", versionDate=" + versionDate + ", dataEntryDate=" + dataEntryDate + '}';
	}
}
