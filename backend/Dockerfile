# syntax=docker/dockerfile:1.6
FROM gradle:8.11.1-jdk21 AS build
WORKDIR /workspace

# Cache dependencies first
COPY settings.gradle.kts build.gradle.kts gradle.properties /workspace/
COPY backend/build.gradle.kts /workspace/backend/
RUN --mount=type=cache,target=/home/gradle/.gradle \
    gradle -p backend --no-daemon dependencies > /dev/null 2>&1 || true

COPY backend/src /workspace/backend/src
RUN --mount=type=cache,target=/home/gradle/.gradle \
    gradle -p backend --no-daemon clean installDist -x test

FROM eclipse-temurin:21-jre-jammy
WORKDIR /opt/app
RUN useradd -r -u 1001 app
COPY --from=build /workspace/backend/build/install/backend /opt/app
RUN chown -R app:app /opt/app
USER app
EXPOSE 8080
ENV JAVA_OPTS="-XX:+UseG1GC -Xms256m -Xmx768m"
ENTRYPOINT ["sh", "-c", "exec /opt/app/bin/backend"]
