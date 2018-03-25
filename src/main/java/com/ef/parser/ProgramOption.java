package com.ef.parser;


public enum ProgramOption {
    
    ACCESS_LOG("accesslog", "Web Server access log file"),
    START_DATE("startDate", "Start date"),
    DURATION("duration", "Duration (hourly or daily)"),
    THRESHOLD("threshold", "Threshold"),
    HELP("help", "show help");
    
    private final String option;
    private final String description;

    private ProgramOption(String option, String description) {
        this.option = option;
        this.description = description;
    }

    public String getOption() {
        return option;
    }

    public String getDescription() {
        return description;
    }
}
