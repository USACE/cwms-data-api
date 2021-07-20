package cwms.radar.data.dto;

import java.util.Date;
import java.util.Objects;
import javax.persistence.Cacheable;
import javax.persistence.Embeddable;
import javax.persistence.Temporal;

@Embeddable
@Cacheable
public class TsvDquId implements java.io.Serializable {

	private String officeId;
	private Long tsCode;
	private String unitId;
	private Date dateTime;

	public TsvDquId() {
	}

	public TsvDquId(String officeId, Long tsCode, String unitId, Date dateTime) {
		this.officeId = officeId;
		this.tsCode = tsCode;
		this.unitId = unitId;
		this.dateTime = dateTime;
	}


	public String getOfficeId() {
		return this.officeId;
	}

	public void setOfficeId(String officeId) {
		this.officeId = officeId;
	}


	public Long getTsCode() {
		return this.tsCode;
	}

	public void setTsCode(Long tsCode) {
		this.tsCode = tsCode;
	}


	public String getUnitId() {
		return this.unitId;
	}

	public void setUnitId(String unitId) {
		this.unitId = unitId;
	}


	@Temporal(javax.persistence.TemporalType.TIMESTAMP)
	public Date getDateTime() {
		return this.dateTime;
	}

	public void setDateTime(Date dateTime) {
		this.dateTime = dateTime;
	}

	@Override
	public int hashCode()
	{
		int hash = 7;
		hash = 41 * hash + Objects.hashCode(this.officeId);
		hash = 41 * hash + Objects.hashCode(this.tsCode);
		hash = 41 * hash + Objects.hashCode(this.unitId);
		hash = 41 * hash + Objects.hashCode(this.dateTime);
		return hash;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof TsvDquId)) {
			return false;
		}
		final TsvDquId other = (TsvDquId) obj;
		if (!Objects.equals(this.officeId, other.officeId)) {
			return false;
		}
		if (!Objects.equals(this.tsCode, other.tsCode)) {
			return false;
		}
		if (!Objects.equals(this.unitId, other.unitId)) {
			return false;
		}
		if (!Objects.equals(this.dateTime, other.dateTime)) {
			return false;
		}
		return true;
	}


	@Override
	public String toString()
	{
		return "TsvDquId{" + "officeId='" + officeId + '\'' + ", tsCode=" + tsCode + ", unitId='" + unitId + '\'' + ", dateTime=" + dateTime + '}';
	}
}
