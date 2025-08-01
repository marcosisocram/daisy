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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    private static HikariDataSource dataSource;
    private static final Logger logger = LoggerFactory.getLogger( Main.class );
    private static final int PROCESSED_AT_DEFAULT = 1;
    private static final int PROCESSED_AT_FALLBACK = 0;

    private static final String INSERT_INTO_DEFAULT_VALUES = "INSERT INTO payments (correlation_id, amount, requested_at, processed_at_default) VALUES ";

    private static StringBuffer INSERTS = new StringBuffer( INSERT_INTO_DEFAULT_VALUES );
    private static final Object INSERTS_LOCK = new Object();

    private static final ArrayDeque< String > DEFAULT_VALUE_FROM = new ArrayDeque<>( );
    private static final ArrayDeque< String > DEFAULT_VALUE_TO = new ArrayDeque<>( );

    private static final AtomicInteger tt =  new AtomicInteger( 0 );

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
            logger.atInfo( ).log( "HikariDataSource created - {}", jdbc );
        }
        return dataSource;
    }

    public static void main( String[] args ) {

        System.setProperty( "org.jboss.logging.provider", "slf4j" );

        logger.atInfo().log( TimeZone.getDefault().getID() );
        logger.atInfo().log( TimeZone.getDefault().getDisplayName() );
        logger.atInfo().log( LocalDateTime.now().toString() );
        logger.atInfo().log( Instant.now().toString() );


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

        DEFAULT_VALUE_FROM.push( Instant.now( ).minus( 1, ChronoUnit.DAYS ).toString( ) );
        DEFAULT_VALUE_TO.push( Instant.now( ).plus( 1, ChronoUnit.DAYS ).toString( ) );

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


        int numberOfConsumers = Math.max( 10 * Runtime.getRuntime( ).availableProcessors( ), 10 );

        for ( int i = 0; i < numberOfConsumers; i++ ) {

            logger.atInfo( ).log( "Consumer {} starting...", i );

            Thread.ofVirtual( ).name( "consumer-" + i ).start( ( ) -> {

                try ( HttpClient httpClient = HttpClient.newHttpClient( ) ) {

                    while ( true ) {
                        try {
                            final Payment takk = queue.take( );

                            final String writeValueAsString = objectMapper.writeValueAsString( takk );

                            try {
                                final HttpRequest request = HttpRequest.newBuilder( )
                                        .uri( URI.create( urlDefault ) )
                                        .header( "Content-Type", "application/json" )
                                        .POST( HttpRequest.BodyPublishers.ofString( writeValueAsString ) )
                                        .build( );

                                HttpResponse< String > httpResponse = httpClient.send( request, HttpResponse.BodyHandlers.ofString( ) );

                                if ( httpResponse.statusCode( ) == 200 ) {
                                    insert( takk, PROCESSED_AT_DEFAULT );
                                } else if ( httpResponse.statusCode( ) == 422 ) {
                                    logger.atError( ).log( "default 422 - {} - {}", writeValueAsString, httpResponse.body( ) );
                                } else {
//                                    logger.atError( ).log( "default Não deu exception, mas retornou {} - {} - {}", httpResponse.statusCode( ), httpResponse.body( ), writeValueAsString );
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
//                                        logger.atError( ).log( "falback Não deu exception, mas retornou {} - {}", httpResponse.statusCode( ), httpResponse.body( ) );
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

        Thread.ofPlatform().name("batch-processor").start(() -> {
            while (true) {
                try {

                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e) {
                    logger.atError().log("Batch processor interrupted");
                }

                String batchSql;
                synchronized (INSERTS_LOCK) {

                    if (INSERTS.toString().equals(INSERT_INTO_DEFAULT_VALUES)) {
                        continue;
                    }

                    batchSql = INSERTS.substring(0, INSERTS.length() - 2);

                    INSERTS.delete(0, INSERTS.length());
                    INSERTS.append(INSERT_INTO_DEFAULT_VALUES);
                }

                try (Connection conn = Main.getDataSource().getConnection();
                     Statement statement = conn.createStatement()) {

                    statement.addBatch(batchSql);
                    statement.executeBatch();

                } catch (SQLException e) {
                    logger.atError().log("Batch processing failed: {}", e.getMessage());
                    logger.atInfo().log("Failed SQL: {}", batchSql);

                    synchronized (INSERTS_LOCK) {

                        if (INSERTS.toString().equals(INSERT_INTO_DEFAULT_VALUES)) {
                            INSERTS = new StringBuffer(INSERT_INTO_DEFAULT_VALUES + batchSql + ", ");
                        } else {
                            INSERTS.append( batchSql ).append( ", " );
                        }
                    }

                    try {
                        TimeUnit.SECONDS.sleep(2);
                    } catch (InterruptedException ie) {
                        logger.atError().log("Interrupted during error backoff");
                    }
                }
            }
        });

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
                            logger.atError( ).log( "Interrupted during error backoff" );
                            throw new RuntimeException( e );
                        }

                    } );
                } )
                .addExactPath( "/payments-summary", exchange -> {



                    Deque< String > fromDeque = exchange.getQueryParameters( ).getOrDefault( "from", DEFAULT_VALUE_FROM );
                    Deque< String > toDeque = exchange.getQueryParameters( ).getOrDefault( "to", DEFAULT_VALUE_TO );

                    final String from = fromDeque.getFirst( );
                    final String to = toDeque.getFirst( );

                    logger.atInfo().log(  "from: {}, to: {}", from, to );

                    exchange.getResponseHeaders( ).put( Headers.CONTENT_TYPE, "application/json" );

                    try (Connection conn = Main.getDataSource( ).getConnection( );
                         Statement statement = conn.createStatement( );
                         ResultSet resultSet = statement.executeQuery( """
                                  select count(*) as total from payments;
                                  """ )) {

                        while (resultSet.next()) {
                            logger.atInfo().log(  "total: {}", resultSet.getInt("total") );
                        }
                    }

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
                            logger.atInfo().log(  "Achou nada");
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
                        logger.atInfo().log(  "Default preenchido");
                        //fallback
                        if ( resultSet.next( ) ) {
                            stringBuilder.append( """
                                        "fallback": {
                                            "totalAmount": %s,
                                            "totalRequests": %s
                                        }
                                    """.formatted( resultSet.getDouble( "totals" ),
                                    resultSet.getInt( "requests" ) ) );
                            logger.atInfo().log(  "Fallback preenchido");
                        } else {
                            stringBuilder.append( """
                                    "fallback": {
                                            "totalAmount": 0,
                                            "totalRequests": 0.0
                                        }
                                    """ );
                            logger.atInfo().log(  "Fallback vazio");
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

        try(HttpClient httpClient = HttpClient.newHttpClient( )) {
            //Request de teste para saber se o problema é bater no default
            final HttpRequest request = HttpRequest.newBuilder( )
                    .uri( URI.create( urlDefault ) )
                    .header( "Content-Type", "application/json" )
                    .POST( HttpRequest.BodyPublishers.ofString( """
                            {"correlationId":"%s","amount":19.9,"requestedAt":"2025-06-31T02:50:52.175762Z"}
                            """.formatted( UUID.randomUUID().toString() )  ) )
                    .build( );

            HttpResponse< String > httpResponse = httpClient.send( request, HttpResponse.BodyHandlers.ofString( ) );

            if (httpResponse.statusCode( ) == 200 ) {
                logger.atInfo( ).log( "Tudo certo com o default" );
            }else {
                logger.atInfo( ).log( "Default com erro - {}", urlDefault );
            }
        } catch ( IOException | InterruptedException e ) {
            logger.atError( ).log( "Default com exception - {}", e.getMessage( ) );
        }

        try(HttpClient httpClient = HttpClient.newHttpClient( )) {
            //Request de teste para saber se o problema é bater no default
            final HttpRequest request = HttpRequest.newBuilder( )
                    .uri( URI.create( urlFallback ) )
                    .header( "Content-Type", "application/json" )
                    .POST( HttpRequest.BodyPublishers.ofString( """
                            {"correlationId":"%s","amount":19.9,"requestedAt":"2025-06-31T02:50:52.175762Z"}
                            """.formatted( UUID.randomUUID().toString() ) ) )
                    .build( );

            HttpResponse< String > httpResponse = httpClient.send( request, HttpResponse.BodyHandlers.ofString( ) );

            if (httpResponse.statusCode( ) == 200 ) {
                logger.atInfo( ).log( "Tudo certo com o fallback" );
            }else {
                logger.atInfo( ).log( "Fallback com erro - {}", urlFallback );
            }
        } catch ( IOException | InterruptedException e ) {
            logger.atError( ).log( "Fallback com exception - {}", e.getMessage( ) );
        }

        logger.atInfo( ).log( "Server started on http://localhost:8080" );
    }

    private static void insert(Payment takk, int processedAtDefault) {

        if (tt.get() % 2000 == 0) {
            logger.atInfo( ).log(  "{} - {}", takk.getRequestedAt().toString( ), takk.getRequestedAt().atZone( ZoneId.systemDefault() ).toString( ) );
        }

        synchronized (INSERTS_LOCK) {
            INSERTS.append(String.format("('%s', %s, '%s', %s), ",
                takk.getCorrelationId(), takk.getAmount(), takk.getRequestedAt().toString(), processedAtDefault));

            tt.incrementAndGet( );
        }
    }
}