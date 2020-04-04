FROM        quay.io/prometheus/busybox:latest
LABEL maintainer "The Prometheus Authors <prometheus-developers@googlegroups.com>"

COPY pushgateway /bin/pushgateway

RUN mkdir -p /pushgateway
WORKDIR    /pushgateway
ENTRYPOINT [ "/bin/pushgateway" ]
