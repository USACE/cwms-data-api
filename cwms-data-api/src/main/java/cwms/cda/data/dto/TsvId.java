package cwms.cda.data.dto;

import java.util.Date;
import java.util.Objects;

public class TsvId implements java.io.Serializable {

	private final Long tsCode;
	private final Date dateTime;
	private final Date versionDate;
	private final Date dataEntryDate;

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

	public Date getDateTime() {
		return this.dateTime;
	}

	public Date getVersionDate() {
		return this.versionDate;
	}

	public Date getDataEntryDate() {
		return this.dataEntryDate;
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
