package cwms.cda.data.dto;

import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import usace.cwms.db.dao.ifc.pool.PoolNameType;

@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.DEFAULT, Formats.JSON})
public class Pool extends CwmsDTOBase {

    private final PoolNameType poolName;
    private final String projectId;
    private final String bottomLevelId;
    private final String topLevelId;
    private final boolean implicit;
    private final Number attribute;
    private final String description;
    private final String clobText;

    private Pool(Builder b) {
        this.poolName = b.getPoolName();
        this.projectId = b.getProjectId();
        this.bottomLevelId = b.getBottomLevelId();
        this.topLevelId = b.getTopLevelId();
        this.implicit = b.isImplicit();
        this.attribute = b.getAttribute();
        this.description = b.getDescription();
        this.clobText = b.getClobText();
    }

    public boolean isImplicit() {
        return this.implicit;
    }

    /**
     * Returns the pool name information for the pool
     *
     * @return Pool name information for the pool
     */
    public PoolNameType getPoolName() {
        return this.poolName;
    }

    /**
     * Returns the Project ID for the pool
     *
     * @return Project ID as a String
     */
    public String getProjectId() {
        return this.projectId;
    }

    /**
     * Returns the bottom location level ID for the pool.
     * <p>
     * This is represented as: {Project}.{Parameter}.{ParameterType}.{Duration}.{SpecifiedLevel}
     *
     * @return String that represents the bottom location level ID.
     */
    public String getBottomLevelId() {
        return this.bottomLevelId;
    }

    /**
     * Returns the top location level ID for the pool.
     * <p>
     * This is represented as: {Project}.{Parameter}.{ParameterType}.{Duration}.{SpecifiedLevel}
     *
     * @return String that represents the top location level ID.
     */
    public String getTopLevelId() {
        return this.topLevelId;
    }

    public Number getAttribute() {
        return attribute;
    }


    public String getDescription() {
        return description;
    }

    public String getClobText() {
        return clobText;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        final Pool pool = (Pool) o;

        if (getAttribute() != null ? !getAttribute().equals(pool.getAttribute()) : pool.getAttribute() != null) {
            return false;
        }
        if (getDescription() != null ? !getDescription().equals(pool.getDescription()) :
            pool.getDescription() != null) {
            return false;
        }

        return getClobText() != null ? getClobText().equals(pool.getClobText()) : pool.getClobText() == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (getAttribute() != null ? getAttribute().hashCode() : 0);
        result = 31 * result + (getDescription() != null ? getDescription().hashCode() : 0);

        result = 31 * result + (getClobText() != null ? getClobText().hashCode() : 0);
        return result;
    }

    public static Pool fromString(String input) {
        Pool retval = null;
        Pattern pattern = Pattern.compile("^(.*)/(.*):(.*)$");
        Matcher matcher = pattern.matcher(input);

        if (matcher.matches()) {
            PoolNameType poolName = new PoolNameType(matcher.group(3), matcher.group(1));
            Builder builder = Builder.newInstance();
            builder.withPoolName(poolName);
            builder.withProjectId(matcher.group(2));
            builder.withBottomLevelId(null);
            builder.withTopLevelId(null);
            builder.withImplicit(true);
            retval = builder.build();
        }

        return retval;
    }

    // This is used in the Pools cursor.
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        PoolNameType poolName = getPoolName();

        builder.append(poolName.getOfficeId())
            .append("/").append(getProjectId())
            .append(":").append(poolName.getPoolName());
        return builder.toString();
    }

    public static class Builder {
        private PoolNameType poolName;

        private String projectId;
        private String bottomLevelId;
        private String topLevelId;
        private boolean isImplicit;
        private Number attribute;
        private String description;
        private String clobText;


        public PoolNameType getPoolName() {
            return poolName;
        }

        public Builder withPoolName(PoolNameType poolName) {
            this.poolName = poolName;
            return this;
        }

        public String getProjectId() {
            return projectId;
        }

        public Builder withProjectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public String getBottomLevelId() {
            return bottomLevelId;
        }

        public Builder withBottomLevelId(String bottomLevelId) {
            this.bottomLevelId = bottomLevelId;
            return this;

        }

        public String getTopLevelId() {
            return topLevelId;
        }

        public Builder withTopLevelId(String topLevelId) {
            this.topLevelId = topLevelId;
            return this;
        }

        public boolean isImplicit() {
            return isImplicit;
        }

        public Builder withImplicit(boolean implicit) {
            isImplicit = implicit;
            return this;
        }

        public Number getAttribute() {
            return attribute;
        }

        public Builder withAttribute(Number attribute) {
            this.attribute = attribute;
            return this;
        }

        public String getDescription() {
            return description;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public String getClobText() {
            return clobText;
        }

        public Builder withClobText(String clobText) {
            this.clobText = clobText;
            return this;
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Pool build() {
            return new Pool(this);
        }
    }
}
