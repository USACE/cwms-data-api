package cwms.cda.formatters.csv;

import cwms.cda.formatters.DateFormat;
import cwms.cda.formatters.DateFormatResolver;

public final class CsvConfiguration {
    private final boolean includeMetadata;
    private final boolean includeOptionalColumns;
    private final DateFormat dateFormat;

    private CsvConfiguration(Builder builder) {
        this.includeMetadata = builder.includeMetadata;
        this.includeOptionalColumns = builder.includeOptionalColumns;
        this.dateFormat = builder.dateFormat;
    }

    public boolean includeMetadata() {
        return includeMetadata;
    }

    public boolean includeOptionalColumns() {
        return includeOptionalColumns;
    }

    public DateFormat getDateFormat() {
        return dateFormat;
    }

    public static class Builder {
        private boolean includeMetadata = false;
        private boolean includeOptionalColumns = false;
        private DateFormat dateFormat = DateFormat.pattern(DateFormatResolver.ISO_INSTANT_PATTERN);

        public Builder withMetadataIncluded(boolean includeMetadata) {
            this.includeMetadata = includeMetadata;
            return this;
        }

        public Builder withOptionalColumnsIncluded(boolean includeOptionalColumns) {
            this.includeOptionalColumns = includeOptionalColumns;
            return this;
        }

        public Builder withDateFormat(DateFormat dateFormat) {
            this.dateFormat = dateFormat;
            return this;
        }

        /**
         * load an existing configuration.
         * @param config existing configuration.
         * @return this builder
         */
        public Builder from(CsvConfiguration config) {
            this.includeMetadata = config.includeMetadata;
            this.includeOptionalColumns = config.includeOptionalColumns;
            this.dateFormat = config.dateFormat;
            return this;
        }

        public CsvConfiguration build() {
            return new CsvConfiguration(this);
        }
    }
}
