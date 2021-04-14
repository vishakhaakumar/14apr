FROM yg397/thrift-microservice-deps:xenial

ARG NUM_CPUS=4

COPY ./ /movies-microservices
RUN cd /movies-microservices \
    && mkdir -p build \
    && cd build \
    && cmake .. \
    && make -j${NUM_CPUS} \
    && make install

WORKDIR /movies-microservices
