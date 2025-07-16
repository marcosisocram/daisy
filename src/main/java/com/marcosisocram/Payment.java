package com.marcosisocram;

import java.io.Serial;
import java.io.Serializable;
import java.time.LocalDateTime;

public class Payment implements Serializable {

    @Serial
    private static final long serialVersionUID = - 4743277877732906325L;

    private String correlationId;
    private Double amount;
    private LocalDateTime requestedAt;

    public Payment( ) {
        this( "", 0d, LocalDateTime.now( ) );
    }

    public Payment( String correlationId, Double amount, LocalDateTime requestedAt ) {
        this.correlationId = correlationId;
        this.amount = amount;
        this.requestedAt = requestedAt;
    }

    public String getCorrelationId( ) {
        return correlationId;
    }

    public void setCorrelationId( String correlationId ) {
        this.correlationId = correlationId;
    }

    public Double getAmount( ) {
        return amount;
    }

    public void setAmount( Double amount ) {
        this.amount = amount;
    }

    public LocalDateTime getRequestedAt( ) {
        return requestedAt;
    }

    public void setRequestedAt( LocalDateTime requestedAt ) {
        this.requestedAt = requestedAt;
    }

    @Override
    public String toString( ) {
        return "Payment{" +
                "correlationId='" + correlationId + '\'' +
                ", amount=" + amount +
                ", requestedAt=" + requestedAt +
                '}';
    }
}
