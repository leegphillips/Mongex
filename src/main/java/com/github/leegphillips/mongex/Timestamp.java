package com.github.leegphillips.mongex;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Timestamp {
    public static final String ATTR_NAME = "timestamp";

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    public static String format(LocalDateTime timestamp) {
        return FORMATTER.format(timestamp);
    }
}
