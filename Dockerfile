FROM marcosisocram/graalvm-oracle-maven:latest AS builder

LABEL authors="marcosisocram@gmail.com"

COPY pom.xml pom.xml
COPY src src
COPY default.iprof default.iprof

RUN mvn native:compile -Pdocker-optimized

FROM gcr.io/distroless/java-base-debian12

WORKDIR /app

COPY --from=builder /opt/app/target/app /app/app
COPY --from=builder /opt/app/target/libsqlitejdbc.so /app/libsqlitejdbc.so

EXPOSE 8080

ENTRYPOINT ["/app/app"]