### 1. STAGE (BUILD)
# Ubuntu 16.04 LTS with Baseimage and Runit
FROM phusion/baseimage:0.10.1 AS builder

# mapping core git branch
ARG MAPPING_CORE_VERSION=master

WORKDIR /app

# copy files
COPY cmake mapping-gfbio/cmake
COPY conf mapping-gfbio/conf
COPY docker-files mapping-gfbio/docker-files
COPY src mapping-gfbio/src
COPY test mapping-gfbio/test
COPY CMakeLists.txt mapping-gfbio/CMakeLists.txt

# set terminal to noninteractive
ARG DEBIAN_FRONTEND=noninteractive

# update packages and upgrade system
RUN apt-get update && \
    apt-get upgrade --yes -o Dpkg::Options::="--force-confold"

# install git and grab mapping-core
RUN apt-get install --yes git && \
    git clone --depth 1 --branch $MAPPING_CORE_VERSION https://github.com/umr-dbs/mapping-core.git

# install OpenCL
RUN chmod +x mapping-core/docker-files/install-opencl-build.sh && \
    mapping-core/docker-files/install-opencl-build.sh

# install MAPPING dependencies
RUN chmod +x mapping-core/docker-files/ppas.sh && \
    mapping-core/docker-files/ppas.sh && \
    python3 mapping-core/docker-files/read_dependencies.py mapping-core/docker-files/dependencies.csv "build dependencies" \
        | xargs -d '\n' -- apt-get install --yes

# install MAPPING GFBIO dependencies
RUN python3 mapping-core/docker-files/read_dependencies.py mapping-gfbio/docker-files/dependencies.csv "build dependencies" \
        | xargs -d '\n' -- apt-get install --yes

# Build MAPPING
RUN cd mapping-core && \
    cmake -DCMAKE_BUILD_TYPE=Release -DMAPPING_MODULES=mapping-gfbio . && \
    make -j$(cat /proc/cpuinfo | grep processor | wc -l)


### 2. STAGE (RUNTIME)
# Ubuntu 16.04 LTS with Baseimage and Runit
FROM phusion/baseimage:0.10.1

WORKDIR /app

COPY --from=builder /app/mapping-core/target/bin /app
COPY --from=builder \
    /app/mapping-core/docker-files \
    /app/mapping-gfbio/docker-files \
    /app/docker-files/

# set terminal to noninteractive
ARG DEBIAN_FRONTEND=noninteractive

RUN \
    # update packages and upgrade system
    apt-get update && \
    apt-get upgrade --yes -o Dpkg::Options::="--force-confold" && \
    # install OpenCL
    chmod +x docker-files/install-opencl-runtime.sh && \
    docker-files/install-opencl-runtime.sh && \
    # install MAPPING dependencies
    chmod +x docker-files/ppas.sh && \
    docker-files/ppas.sh && \
    python3 docker-files/read_dependencies.py docker-files/dependencies.csv "runtime dependencies" \
        | xargs -d '\n' -- apt-get install --yes && \
    # install MAPPING GFBIO dependencies
    python3 docker-files/read_dependencies.py docker-files/gfbio-dependencies.csv "runtime dependencies" \
            | xargs -d '\n' -- apt-get install --yes && \
    # Make mountable files and give rights to www-data
    chown www-data:www-data . && \
    touch userdb.sqlite && \
    chown www-data:www-data userdb.sqlite && \
    mkdir gdalsources_data && \
    chown www-data:www-data gdalsources_data && \
    mkdir gdalsources_description && \
    chown www-data:www-data gdalsources_description && \
    mkdir ogrsources_data && \
    chown www-data:www-data ogrsources_data && \
    mkdir ogrsources_description && \
    chown www-data:www-data ogrsources_description && \
    # module mounts
    mkdir abcd_files && \
    chown www-data:www-data abcd_files && \
    # Make service available
    mkdir --parents /etc/service/mapping/ && \
    mv docker-files/mapping-service.sh /etc/service/mapping/run && \
    chmod +x /etc/service/mapping/run && \
    ln -sfT /dev/stderr /var/log/mapping.log && \
    # Clean APT and install scripts
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /app/docker-files

# Make port 10100 available to the world outside this container
EXPOSE 10100

# Expose mountable volumes
VOLUME /app/gdalsources_data \
       /app/gdalsources_description \
       # /app/userdb.sqlite \
       # /app/conf/settings.toml \
       /app/ogrsources_data \
       /app/ogrsources_description \
       # module mounts
       /app/abcd_files

# Use baseimage-docker's init system.
CMD ["/sbin/my_init"]
