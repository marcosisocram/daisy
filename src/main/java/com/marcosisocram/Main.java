package com.marcosisocram;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.marcosisocram.utils.Log;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.Headers;

public class Main {
    public static void main( String[] args ) {

        ObjectMapper objectMapper = new ObjectMapper( );
        SimpleModule simpleModule = new SimpleModule( );
        simpleModule.addDeserializer( Payment.class, new PaymentDeserializer( ) );

        objectMapper.registerModule( simpleModule );

        Log.info( "Server starting..." );

        HttpHandler defaultHandler = exchange -> {
            exchange.getResponseHeaders( ).put( Headers.CONTENT_TYPE, "text/plain" );
            exchange.getResponseSender( ).send( "Default handler: Path not found." );
        };

        PathHandler pathHandler = new PathHandler( defaultHandler )
                .addExactPath( "/payments", exchange -> {

                    exchange.getRequestReceiver( ).receiveFullString( ( httpServerExchange, s ) -> {
                        try {
                            Payment payment = objectMapper.readValue( s, Payment.class );
                            Log.info( payment.toString( ) );
                        } catch ( JsonProcessingException e ) {
                            Log.info( "JsonProcessingException: " + e.getMessage( ) );
                        }
                    } );

                } )
                .addExactPath( "/payments-summary", exchange -> {

                    exchange.getResponseHeaders( ).put( Headers.CONTENT_TYPE, "application/json" );
                    exchange.getResponseSender( ).send( """
                            {
                              "default": {
                                "totalRequests": 0,
                                "totalAmount": 0.0
                              },
                              "fallback": {
                                "totalRequests": 0,
                                "totalAmount": 0.0
                              }
                            }
                            """ );
                } )
                .addExactPath( "/purge-payments", exchange -> {

                    exchange.getResponseHeaders( ).put( Headers.CONTENT_TYPE, "application/json" );
                    exchange.getResponseSender( ).send( """
                            {
                              "message": "All payments purged."
                            }
                            """ );
                } );

        Undertow server = Undertow.builder( )
                .addHttpListener( 8080, "localhost" )
                .setHandler( pathHandler )
                .build( );

        server.start( );
        Log.info( "Server started on http://localhost:8080" );
    }
}