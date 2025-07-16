package com.marcosisocram;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.time.LocalDateTime;

public class PaymentDeserializer extends JsonDeserializer< Payment > {

    @Override
    public Payment deserialize( JsonParser jp, DeserializationContext ctxt ) throws IOException, JacksonException {

        JsonNode node = jp.getCodec( ).readTree( jp );
        String correlationId = node.get( "correlationId" ).asText( );
        Double amount = node.get( "amount" ).asDouble( );

        return new Payment( correlationId, amount, LocalDateTime.now( ) );
    }
}
