# Variation specific to the deploy environment will go here
ARG IMAGE=ghcr.io/hydrologicengineeringcenter/cwms-database/cwms/schema_installer:latest-dev
FROM ${IMAGE}

COPY entry.sh /
COPY create-queues.sql /after.install.d/
COPY cda-user.sql /after.install.d/
COPY aaaa-updates.sql /after.install.d/

ENV RDS_MODE="true"
ENV BUILDUSER=DBAdmin


ENTRYPOINT [ "/entry.sh" ]
CMD ["/cwmsdb/schema/docker/install.sh"]