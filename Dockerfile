FROM buildpack-deps

COPY . /phxqueue

ENV BUILD_DEPS="cmake python-pip"

RUN apt-get update \
        && apt-get install -y $BUILD_DEPS --no-install-recommends \
        && pip install protobuf \
        && cd /phxqueue \
	&& ./build.sh \
	&& find . -name "*.o" | xargs rm

ENV WORK_DIR=/phxqueue

WORKDIR $WORK_DIR

VOLUME $WORK_DIR/data

ENV PATH="$WORK_DIR/bin:$PATH"

#COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]

#EXPOSE 5100 5200 5300

