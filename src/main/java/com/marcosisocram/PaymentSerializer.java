package com.marcosisocram;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class PaymentSerializer extends JsonSerializer<Payment> {

    @Override
    public void serialize(Payment payment, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("correlationId", payment.getCorrelationId());
        jsonGenerator.writeNumberField("amount", payment.getAmount());
        
        if (payment.getRequestedAt() != null) {
            jsonGenerator.writeStringField("requestedAt", payment.getRequestedAt().toString());
        }
        
        jsonGenerator.writeEndObject();
    }
}