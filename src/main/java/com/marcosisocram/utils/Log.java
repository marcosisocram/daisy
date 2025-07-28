package com.marcosisocram.utils;

import java.time.Instant;

public class Log {

    private Log( ) {
        throw new IllegalStateException( "Utility class" );
    }

    public static void info( String message, Object... args ) {
        Thread thread = Thread.currentThread( );

        System.out.printf( Instant.now( ) + " [" + thread.getName( ) + "] [INFO] - " + message + "%n", args );
    }

    public static void error( String message, Object... args ) {
        Thread thread = Thread.currentThread( );

        System.err.printf( Instant.now( ) + " [" + thread.getName( ) + "] [ERROR] - " + message + "%n", args );
    }
}
