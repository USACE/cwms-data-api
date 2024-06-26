package cwms.cda.data.dto.stream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum Bank {
    LEFT("Left", "L"),
    RIGHT("Right", "R");

    private final String name;
    private final String code;

    Bank(String name, String code){
        this.name = name;
        this.code = code;
    }

    public String getName(){
        return name;
    }

    @JsonValue
    public String getCode(){
        return code;
    }

    public static Bank fromName(String name){
        for(Bank bank : Bank.values()){
            if(bank.getName().equals(name)){
                return bank;
            }
        }
        return null;
    }

    @JsonCreator
    public static Bank fromCode(String code){
        for(Bank bank : Bank.values()){
            if(bank.getCode().equals(code)){
                return bank;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return getName();
    }
}
