FROM golang:1.12.1 as base

RUN mkdir /app 
ADD . /app/ 
WORKDIR /app

RUN make build && export PATH=$PATH:/app
ENTRYPOINT ["/app/cache-sensor"]