FROM ghcr.io/valitydev/temurin-jemalloc:sha-402a042

RUN useradd -s /bin/false -U -u 20000 vality

COPY --chown=vality:vality cc-reporter-1.0.0.jar /opt/cc-reporter/cc-reporter.jar

USER vality:vality

CMD ["java", "-jar","/opt/cc-reporter/cc-reporter.jar"]

EXPOSE 8022 8023

WORKDIR /opt/cc-reporter
