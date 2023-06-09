package cwms.cda.data.dto;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import io.swagger.v3.oas.annotations.media.Schema;
import cwms.cda.api.errors.FieldException;

@XmlRootElement(name="pools")
@XmlSeeAlso(Pool.class)
@XmlAccessorType(XmlAccessType.FIELD)
public class Pools extends CwmsDTOPaginated {
    @XmlElementWrapper
    @XmlElement(name="pool")

    @Schema(description = "List of retrieved pools")
    List<Pool> pools;

    @SuppressWarnings("unused") // for JAXB to handle marshalling
    private Pools(){}

    private int offset;

    public Pools(int offset, int pageSize, Integer total)
    {
        super(Integer.toString(offset), pageSize, total);
        pools = new ArrayList<>();
        this.offset = offset;
    }

    public List<Pool> getPools() {
        return Collections.unmodifiableList(pools);
    }


    public static class Builder {
        private Pools workingPools = null;

        public Builder(int offset, int pageSize, Integer total){
            workingPools = new Pools(offset, pageSize, total);
        }

        public Pools build(){
            if( this.workingPools.pools.size() == this.workingPools.pageSize){

                String cursor = Integer.toString(this.workingPools.offset + this.workingPools.pools.size());
                this.workingPools.nextPage = encodeCursor(cursor,
                            this.workingPools.pageSize,
                            this.workingPools.total);
            } else {
                this.workingPools.nextPage = null;
            }
            return workingPools;
        }

        public Builder add(Pool pool){
            this.workingPools.pools.add(pool);
            return this;
        }

        public Builder addAll(Collection<? extends Pool> pools){
            this.workingPools.pools.addAll(pools);
            return this;
        }
    }


    @Override
    public void validate() throws FieldException {
        // TODO Auto-generated method stub

    }




}
