FROM postgres:9.6

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python2.7 \
      "postgresql-contrib-$PG_MAJOR" \
      "postgresql-plpython-$PG_MAJOR"

RUN apt-get clean \
 && rm -rf /var/cache/apt/* /var/lib/apt/lists/*

COPY docker/docker-entrypoint-initdb.d /docker-entrypoint-initdb.d/
COPY db /db
WORKDIR /db
