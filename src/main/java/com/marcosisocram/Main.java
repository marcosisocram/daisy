package com.marcosisocram;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.util.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {

    private static HikariDataSource dataSource;
    private static final Logger logger = LoggerFactory.getLogger( Main.class );
    private static final int PROCESSED_AT_DEFAULT = 1;
    private static final int PROCESSED_AT_FALLBACK = 0;

    private static final String INSERT_INTO_DEFAULT_VALUES = "INSERT INTO payments (correlation_id, amount, requested_at, processed_at_default) VALUES ";

    private static StringBuffer INSERTS = new StringBuffer( INSERT_INTO_DEFAULT_VALUES );


    public static synchronized DataSource getDataSource( ) {
        if ( dataSource == null ) {
            HikariConfig config = new HikariConfig( );

            String jdbc = Optional.ofNullable( System.getenv( "JDBC" ) ).orElse( "jdbc:sqlite:rinha-daisy.db" );

            config.setJdbcUrl( jdbc );
            config.setDriverClassName( "org.sqlite.JDBC" );
            config.setMaximumPoolSize( 10 );
            config.setMinimumIdle( 2 );
            config.setConnectionTimeout( 30000 );
            config.setIdleTimeout( 600000 );
            config.setMaxLifetime( 1800000 );

            dataSource = new HikariDataSource( config );
            logger.atInfo( ).log( "HikariDataSource created" );
        }
        return dataSource;
    }

    public static void main( String[] args ) {

        System.setProperty( "org.jboss.logging.provider", "slf4j" );

        final String urlDefault = Optional.ofNullable( System.getenv( "URL_DEFAULT" ) ).orElse( "http://localhost:8001/payments" );
        final String urlFallback = Optional.ofNullable( System.getenv( "URL_FALLBACK" ) ).orElse( "http://localhost:8002/payments" );

        final ObjectMapper objectMapper = new ObjectMapper( );
        final SimpleModule simpleModule = new SimpleModule( );
        simpleModule.addDeserializer( Payment.class, new PaymentDeserializer( ) );
        simpleModule.addSerializer( Payment.class, new PaymentSerializer( ) );

        objectMapper.registerModule( simpleModule );
        objectMapper.disable( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS );
        objectMapper.registerModule( new JavaTimeModule( ) );

        final BlockingQueue< Payment > queue = new LinkedBlockingQueue<>( 20_000 );

        logger.atInfo( ).log( "Server starting..." );

        logger.atInfo( ).log( "CPUS - {}", Runtime.getRuntime( ).availableProcessors( ) );

        final String table = """
                CREATE TABLE IF NOT EXISTS payments (
                    correlation_id TEXT PRIMARY KEY,
                    amount NUMERIC NOT NULL,
                    requested_at TEXT NOT NULL,
                    processed_at_default INTEGER NOT NULL DEFAULT 1
                );
                """;

        final String index = "CREATE INDEX payments_requested_at_processed_default ON payments (requested_at, processed_at_default);";

        try ( Connection conn = Main.getDataSource( ).getConnection( );
              Statement statement = conn.createStatement( ) ) {

            statement.executeUpdate( table );
            statement.executeUpdate( index );

        } catch ( SQLException e ) {
            logger.atWarn( ).log( e.getMessage( ) );
        }

//        int numberOfConsumers = ( Runtime.getRuntime( ).availableProcessors( ) / 2 ) + 1;
        int numberOfConsumers = 10;

        for ( int i = 0; i < numberOfConsumers; i++ ) {

            logger.atInfo( ).log( "Consumer {} starting...", i );

            Thread.ofVirtual( ).name( "consumer-" + i ).start( ( ) -> {

                try ( HttpClient httpClient = HttpClient.newHttpClient( ) ) {

                    while ( true ) {
                        try {
                            final Payment takk = queue.take( );
//                            logger.atInfo( ).log( "{}", takk.getCorrelationId( ) );

                            final String writeValueAsString = objectMapper.writeValueAsString( takk );

                            try {
                                final HttpRequest request = HttpRequest.newBuilder( )
                                        .uri( URI.create( urlDefault ) )
//                                        .timeout( Duration.ofMillis( 500 ) )
                                        .header( "Content-Type", "application/json" )
                                        .POST( HttpRequest.BodyPublishers.ofString( writeValueAsString ) )
                                        .build( );

                                HttpResponse< String > httpResponse = httpClient.send( request, HttpResponse.BodyHandlers.ofString( ) );

                                if ( httpResponse.statusCode( ) == 200 ) {
                                    insert( takk, PROCESSED_AT_DEFAULT );
                                } else if ( httpResponse.statusCode( ) == 422 ) {
                                    logger.atError( ).log( "default 422 - {} - {}", writeValueAsString, httpResponse.body( ) );
                                } else {
                                    logger.atError( ).log( "default Não deu exception, mas retornou {} - {} - {}", httpResponse.statusCode( ), httpResponse.body( ), writeValueAsString );
                                    //TODO gerar uma nova imagem e rodar novamente e ver o que deu de 405
                                    queue.put( takk );
                                }

                            } catch ( IOException e ) {
                                logger.atError( ).log( e.getMessage( ), e );

                                try {
                                    final HttpRequest request = HttpRequest.newBuilder( )
                                            .uri( URI.create( urlFallback ) )
                                            .timeout( Duration.ofMillis( 100 ) )
                                            .header( "Content-Type", "application/json" )
                                            .POST( HttpRequest.BodyPublishers.ofString( writeValueAsString ) )
                                            .build( );

                                    final HttpResponse< String > httpResponse = httpClient.send( request, HttpResponse.BodyHandlers.ofString( ) );

                                    if ( httpResponse.statusCode( ) == 200 ) {
                                        insert( takk, PROCESSED_AT_FALLBACK );
                                    } else if ( httpResponse.statusCode( ) == 422 ) {
                                        logger.atError( ).log( "fallback 422 - {}", writeValueAsString );
                                    } else {
                                        logger.atError( ).log( "falback Não deu exception, mas retornou {} - {}", httpResponse.statusCode( ), httpResponse.body( ) );
                                        queue.put( takk );
                                    }

                                } catch ( IOException exception ) {
                                    logger.atError( ).log( "NOK {}", takk.getCorrelationId( ) );
                                    queue.put( takk );
                                }
                            }

                        } catch ( InterruptedException | IOException e ) {
                            logger.atError( ).log( "Parando o while - {}", e.getMessage( ) );
                            throw new RuntimeException( e );
                        }
                    }
                }
            } );
        }

        Thread.ofPlatform( ).start( ( ) -> {

            while ( true ) {

                try {
                    TimeUnit.SECONDS.sleep( 1 );
                } catch ( InterruptedException e ) {
                    logger.atError( ).log( "Interrupted" );
                }

                if ( INSERTS.toString( ).equals( INSERT_INTO_DEFAULT_VALUES ) ) {
                    continue;
                }

                try ( Connection conn = Main.getDataSource( ).getConnection( );
                      Statement statement = conn.createStatement( ) ) {

                    statement.addBatch( INSERTS.substring( 0, INSERTS.length( ) - 2 ) );
                    statement.executeBatch( );

                    INSERTS.delete( 0, INSERTS.length( ) );
                    INSERTS.append( INSERT_INTO_DEFAULT_VALUES );
                    logger.atInfo( ).log( "OK" );
                } catch ( SQLException e ) {
                    logger.atInfo( ).log( INSERTS.substring( 0, INSERTS.length( ) - 2 ) );
                    logger.atError( ).log( "SQL - {}", e.getMessage( ) );
                }
            }
        } );

        //TODO INCOSISTENCIAAAAAAAAAAAAA

        HttpHandler defaultHandler = exchange -> {
            exchange.getResponseHeaders( ).put( Headers.CONTENT_TYPE, "text/plain" );
            exchange.getResponseSender( ).send( "Default handler: Path not found." );
        };

        PathHandler pathHandler = new PathHandler( defaultHandler )
                .addExactPath( "/payments", exchange -> {

                    exchange.getRequestReceiver( ).receiveFullString( ( httpServerExchange, body ) -> {

                        try {
                            Payment payment = objectMapper.readValue( body, Payment.class );
                            payment.setRequestedAt( Instant.now( ) );
                            queue.put( payment );
                        } catch ( JsonProcessingException e ) {
                            logger.atInfo( ).log( "JsonProcessingException: " + e.getMessage( ) );
                        } catch ( InterruptedException e ) {
                            throw new RuntimeException( e );
                        }

                    } );
                } )
                .addExactPath( "/payments-summary", exchange -> {

                    final String from = Optional.ofNullable( exchange.getQueryParameters( ).get( "from" ).getFirst( ) ).orElse( Instant.now( ).minus( 1, ChronoUnit.DAYS ).toString( ) );
                    final String to = Optional.ofNullable( exchange.getQueryParameters( ).get( "to" ).getFirst( ) ).orElse( Instant.now( ).plus( 1, ChronoUnit.DAYS ).toString( ) );

                    exchange.getResponseHeaders( ).put( Headers.CONTENT_TYPE, "application/json" );

                    try ( Connection conn = Main.getDataSource( ).getConnection( );
                          Statement statement = conn.createStatement( );
                          ResultSet resultSet = statement.executeQuery( """
                                  select payments.processed_at_default, sum(payments.amount) as totals, count(payments.correlation_id) as requests from payments 
                                  where requested_at between '%s' and '%s' 
                                  group by payments.processed_at_default 
                                  order by processed_at_default desc;
                                  """.formatted( from, to ) ) ) {

                        final StringBuilder stringBuilder = new StringBuilder( "{" );


                        if ( ! resultSet.next( ) ) {
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
                            return;
                        }

                        stringBuilder.append( """
                                    "default": {
                                        "totalAmount": %s,
                                        "totalRequests": %s
                                    },
                                """.formatted( resultSet.getDouble( "totals" ),
                                resultSet.getInt( "requests" ) ) );

                        //fallback
                        if ( resultSet.next( ) ) {
                            stringBuilder.append( """
                                        "fallback": {
                                            "totalAmount": %s,
                                            "totalRequests": %s
                                        }
                                    """.formatted( resultSet.getDouble( "totals" ),
                                    resultSet.getInt( "requests" ) ) );
                        } else {
                            //não tem fallback
                            stringBuilder.append( """
                                    "fallback": {
                                            "totalAmount": 0,
                                            "totalRequests": 0.0
                                        }
                                    """ );
                        }

                        stringBuilder.append( "}" );
                        exchange.getResponseSender( ).send( stringBuilder.toString( ) );

                    } catch ( SQLException e ) {
                        logger.atError( ).log( e.getMessage( ) );
                    }
                } )
                .addExactPath( "/purge-payments", exchange -> {

                    try ( Connection conn = Main.getDataSource( ).getConnection( );
                          Statement statement = conn.createStatement( ) ) {
                        statement.executeUpdate( "DELETE FROM payments;" );

                    } catch ( SQLException e ) {
                        logger.atError( ).log( e.getMessage( ) );
                    }
                } );

        final Undertow server = Undertow.builder( )
//                .setIoThreads( 2 )
//                .setWorkerThreads( 10 )

                .addHttpListener( 8080, "0.0.0.0" )
                .setHandler( pathHandler )
                .build( );

        server.start( );

        Runtime.getRuntime( ).addShutdownHook( new Thread( ( ) -> {
            server.stop( );
            logger.atInfo( ).log( "Server stopped" );
        } ) );

        logger.atInfo( ).log( "Server started on http://localhost:8080" );
    }

    private static void insert( Payment takk, int processedAtDefault ) {

        INSERTS.append( String.format( "('%s', %s, '%s', %s), ", takk.getCorrelationId( ), takk.getAmount( ), takk.getRequestedAt( ).toString( ), processedAtDefault ) );

    }
}