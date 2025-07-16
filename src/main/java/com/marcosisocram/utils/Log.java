package com.marcosisocram.utils;

import java.time.LocalDateTime;

public class Log {

    private Log () {
        throw new IllegalStateException("Utility class");
    }

    public static void info(String message, Object... args) {
        System.out.printf( LocalDateTime.now() + " [INFO] - "+ message + "%n", args );
    }
}
