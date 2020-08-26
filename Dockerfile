FROM golang:1.13 as builder
WORKDIR /go/src/github.com/retzkek/ingestbeat
RUN go get github.com/magefile/mage
COPY . .
RUN mage build


FROM centos:7

RUN mkdir -p /ingestbeat
RUN useradd -u 1001 -g 0 ingestbeat
COPY --from=builder /go/src/github.com/retzkek/ingestbeat/ingestbeat* /ingestbeat/
# make working directory owned by root group for openshift
# https://docs.openshift.com/container-platform/3.3/creating_images/guidelines.html#openshift-container-platform-specific-guidelines
RUN chown -R ingestbeat:0 /ingestbeat && \
    chmod -R g=u /ingestbeat && \
    chmod go-w /ingestbeat/ingestbeat.yml
WORKDIR /ingestbeat
USER 1001

CMD ["./ingestbeat","-e","-d","*"]
