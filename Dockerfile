FROM marcosisocram/graalvm-nik-maven:latest AS builder

LABEL authors="marcosisocram@gmail.com"

COPY pom.xml pom.xml
COPY src src

RUN mvn native:compile

FROM gcr.io/distroless/java-base-debian12

WORKDIR /app

COPY --from=builder /opt/app/target/app /app/app
COPY --from=builder /opt/app/target/libsqlitejdbc.so /app/libsqlitejdbc.so

EXPOSE 8080

ENTRYPOINT ["/app/app"]