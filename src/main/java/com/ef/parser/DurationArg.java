package com.ef.parser;


public enum DurationArg {
    
    HOURLY("hourly"),
    DAILY("daily");
    
    final private String value;

    private DurationArg(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }    
}
