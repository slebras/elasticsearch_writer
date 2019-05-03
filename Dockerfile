FROM python:3.7-slim
WORKDIR /app

ARG DEVELOPMENT
ARG BUILD_DATE
ARG VCS_REF
ARG BRANCH=develop

# Copy required files
COPY ./requirements.txt ./dev-requirements.txt ./src/ ./tox.ini /app/

# Install pip requirements
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    if [ "$DEVELOPMENT" ]; then pip install --no-cache-dir -r dev-requirements.txt; fi

# Install dockerize
RUN apt-get update && apt-get install -y wget
ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/kbase/dockerize/raw/master/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    tar -C /usr/local/bin -xvzf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.vcs-url="https://github.com/kbaseincubator/elasticsearch_writer" \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.schema-version="1.0.0-rc1" \
      us.kbase.vcs-branch=$BRANCH \
      maintainer="KBase Team"

ENTRYPOINT ["/usr/local/bin/dockerize"]
CMD ["python", "-m", "src.main"]
