package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@JsonRootName("pools")
@FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class Pools extends CwmsDTOPaginated {
    @JacksonXmlElementWrapper
    @JacksonXmlProperty(localName = "pool")

    @Schema(description = "List of retrieved pools")
    List<Pool> pools;

    @SuppressWarnings("unused") // for JAXB to handle marshalling
    private Pools() {
    }

    private int offset;

    public Pools(int offset, int pageSize, Integer total) {
        super(Integer.toString(offset), pageSize, total);
        pools = new ArrayList<>();
        this.offset = offset;
    }

    public List<Pool> getPools() {
        return Collections.unmodifiableList(pools);
    }


    public static class Builder {
        private Pools workingPools;

        public Builder(int offset, int pageSize, Integer total) {
            workingPools = new Pools(offset, pageSize, total);
        }

        public Pools build() {
            if (this.workingPools.pools.size() == this.workingPools.pageSize) {

                String cursor =
                        Integer.toString(this.workingPools.offset + this.workingPools.pools.size());
                this.workingPools.nextPage = encodeCursor(cursor,
                        this.workingPools.pageSize,
                        this.workingPools.total);
            } else {
                this.workingPools.nextPage = null;
            }
            return workingPools;
        }

        public Builder add(Pool pool) {
            this.workingPools.pools.add(pool);
            return this;
        }

        public Builder addAll(Collection<? extends Pool> pools) {
            this.workingPools.pools.addAll(pools);
            return this;
        }
    }
}
