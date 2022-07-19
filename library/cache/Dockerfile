FROM golang:1.12.5
#FROM alpine
MAINTAINER Jason
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime &&   echo $TZ > /etc/timezone
#RUN apk update \
#    && apk add tzdata \
#    && cp /usr/share/zoneinfo/$TZ /etc/localtime \
#    && echo $TZ > /etc/timezone
WORKDIR /uac/src/library/cache
CMD exec /bin/bash -c "trap : TERM INT; sleep infinity &amp; wait"
#CMD exec /bin/sh -c "trap : TERM INT; (while true; do sleep 1000; done) &amp; wait"