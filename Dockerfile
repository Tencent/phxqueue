FROM buildpack-deps:zesty

COPY . /phxqueue

RUN apt-get update \
	&& apt-get install -y cmake --no-install-recommends \
	&& cd /phxqueue \
	&& ./build.sh \
	&& apt-get purge -y --auto-remove cmake

WORKDIR /phxqueue

ENV PATH="/phxqueue/bin:$PATH"

COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]

#EXPOSE 5100 5200 5300

#VOLUME /data

