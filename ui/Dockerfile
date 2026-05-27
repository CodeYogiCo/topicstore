# syntax=docker/dockerfile:1.6
FROM gradle:8.11.1-jdk21 AS build
WORKDIR /workspace

COPY settings.gradle.kts build.gradle.kts gradle.properties /workspace/
COPY ui/build.gradle.kts /workspace/ui/
COPY backend/build.gradle.kts /workspace/backend/
RUN --mount=type=cache,target=/home/gradle/.gradle \
    gradle -p ui --no-daemon dependencies > /dev/null 2>&1 || true

COPY ui/src /workspace/ui/src
RUN --mount=type=cache,target=/home/gradle/.gradle \
    --mount=type=cache,target=/workspace/.kotlin \
    --mount=type=cache,target=/workspace/kotlin-js-store \
    gradle -p ui --no-daemon clean jsBrowserDistribution

FROM nginx:1.27-alpine
COPY ui/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /workspace/ui/build/dist/js/productionExecutable/ /usr/share/nginx/html/
EXPOSE 80
HEALTHCHECK --interval=10s --timeout=2s --start-period=3s --retries=3 \
    CMD wget -q -O- http://127.0.0.1/ >/dev/null || exit 1
