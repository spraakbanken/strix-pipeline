FROM python:3.4-alpine
#RUN apk add --no-cache make gcc g++ python

# Create app directory
WORKDIR /usr/src/app

RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh

# Install app dependencies
COPY . .
# won't work because the build context is the current folder.
# COPY ../strix-config-configurer .

RUN pip install -e .
# doesn't work because we can't download from private repos
# RUN pip install -e git+git@github.com:spraakbanken/strix-config-configurer.git#egg=strix-config-configurer


# TODO: fix urls here
RUN echo "elastic_hosts: [{host: $DOCKER_ELASTIC_HOST, port: 9200}] \
texts_dir: /home/strix/texts \
settings_dir: /home/strix/settings \
base_dir: . \
concurrency_upload_threads: 40 \
concurrency_queue_size: 60 \
concurrency_group_size: 20" > config.yml




#EXPOSE 8080
# CMD [ "python", "./bin/strix-api.py", "8080"]