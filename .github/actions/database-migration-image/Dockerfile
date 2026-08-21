# Variation specific to the deploy environment will go here
ARG IMAGE=ghcr.io/hydrologicengineeringcenter/cwms-database/cwms/schema_installer:latest-dev
FROM ${IMAGE}

COPY entry.sh /
COPY create-queues.sql /after.install.d/
COPY cda-user.sql /after.install.d/


ENV RDS_MODE="true"
ENV BUILDUSER=DBAdmin

# Due to the way we originally set this up it needs to always be 99.99.99-SNAPSHOT for now.
RUN sed -i "s/<revision>.*<\/revision>/<revision>99.99.99-SNAPSHOT<\/revision>/" ../pom.xml

ENTRYPOINT [ "/entry.sh" ]
CMD ["/cwmsdb/schema/docker/install.sh"]
