ARG PULSAR_FQ_IMAGE
FROM ${PULSAR_FQ_IMAGE}

ARG OXIA_VERSION

USER root
RUN rm -rf /pulsar/lib/pulsar-metadatastore-oxia-*-shaded.jar

COPY ../pulsar-metadatastore-oxia/target/pulsar-metadatastore-oxia-${OXIA_VERSION}-shaded.jar /pulsar/lib