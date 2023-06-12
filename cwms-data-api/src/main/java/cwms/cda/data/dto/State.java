/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto;

import cwms.cda.api.errors.FieldException;
import cwms.cda.api.errors.RequiredFieldException;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

@Schema(description = "A representation of a state")
@XmlRootElement(name="state")
@XmlAccessorType(XmlAccessType.FIELD)
public class State implements CwmsDTOBase {
    @XmlElement(name="state-initial")
    private String stateInitial;
    @XmlElement(name="name")
    private String name;

    public State(){}

    public State(String stateInitial, String name){
        this.stateInitial = stateInitial;
        this.name = name;
    }

    public String getName(){ return name; }

    public String getStateInitial() {
        return stateInitial;
    }

    @Override
    public void validate() throws FieldException {
        ArrayList<String> missingFields = new ArrayList<>();
        if (this.getName() == null) {
            missingFields.add("Name");
        }
        if (this.getStateInitial() == null) {
            missingFields.add("State Initial");
        }
        if (!missingFields.isEmpty()) {
            throw new RequiredFieldException(missingFields);
        }
    }
}
