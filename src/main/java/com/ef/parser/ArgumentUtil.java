package com.ef.parser;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;


public class ArgumentUtil {

    private static final String DATE_FORMAT_ARG = "yyyy-MM-dd.HH:mm:ss";

    private ArgumentUtil() {
    }
    
    public static File parseAccessFile(String acessFileArg) {
        File file = new File(acessFileArg);
        
        if(!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File not exists or it is a directory");
        }
        
        return file;
    }
    
    public static LocalDateTime parseStartDate(String startDateArg) {
        try {
            return LocalDateTime.parse(startDateArg, DateTimeFormatter.ofPattern(DATE_FORMAT_ARG));
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
    
    public static DurationArg parseDuration(String durationArg) {
        return DurationArg.valueOf(durationArg.toUpperCase()); // throws IllegalArgumentException
    }

    public static int parseThreshold(String thresholdArg) {
        try {
            return Integer.parseInt(thresholdArg);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
    
    public static LocalDateTime getFinalDate(LocalDateTime startDate, DurationArg duration) {
        switch (duration) {
            case HOURLY:
                return LocalDateTime.from(startDate).plusHours(1);
            case DAILY:
                return LocalDateTime.from(startDate).plusDays(1);                
            default:
                throw new IllegalArgumentException("");
        }
    }
}
