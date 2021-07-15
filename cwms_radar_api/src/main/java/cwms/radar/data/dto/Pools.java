package cwms.radar.data.dto;

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
import org.jetbrains.annotations.NotNull;

@XmlRootElement(name="pools")
@XmlSeeAlso(Pool.class)
@XmlAccessorType(XmlAccessType.FIELD)
public class Pools extends CwmsDTOPaginated {
    @XmlElementWrapper
    @XmlElement(name="pool")

    @Schema(implementation = Pool.class, description = "List of retrieved pools")
    List<Pool> pools;

    @SuppressWarnings("unused") // for JAXB to handle marshalling
    private Pools(){}


    private Pools(String cursor, int pageSize, int total){
        super(cursor, pageSize, total);
        pools = new ArrayList<>();
    }

    public List<Pool> getPools() {
        return Collections.unmodifiableList(pools);
    }


    public static class Builder {
        private Pools workingPools = null;

        public Builder( Pool lastItem, int pageSize, int total){
            workingPools = new Pools(encodeItem(lastItem), pageSize, total);
        }

        public Builder( String cursor, int pageSize, int total){
            workingPools = new Pools(cursor, pageSize, total);
        }

        public Pools build(){
            if( this.workingPools.pools.size() == this.workingPools.pageSize){
                Pool lastItem = this.workingPools.pools.get(this.workingPools.pools.size() - 1);
                this.workingPools.nextPage = encodeCursor(Pools.encodeItem(lastItem),
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

    public static String encodeItem(Pool lastItem)
    {
        String retval = null;
        if(lastItem != null)
        {
            retval = lastItem.toString().toUpperCase();
        }
        return retval;
    }

    public static Pool decodeItem(String itemPortion){
        return Pool.fromString(itemPortion);
    }

}
