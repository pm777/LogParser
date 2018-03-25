package com.ef.parser;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;


public interface Processor {

    Map<String, Integer> findIps(File file, LocalDateTime startDate, LocalDateTime finalDate, int threshold) throws IOException;
    
}
