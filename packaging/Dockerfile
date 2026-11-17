# QDK (QNAP QPKG Development Kit) builder image.
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential bsdmainutils rsync openssl python3 git ca-certificates xz-utils \
 && rm -rf /var/lib/apt/lists/*
ARG QDK_COMMIT=b7b5f4c86ebe95b10a62d64725515e7cdcf4bb35
RUN git clone https://github.com/qnap-dev/QDK.git /opt/QDK \
 && cd /opt/QDK && git checkout "$QDK_COMMIT" \
 && make -C src && cp src/bin/qpkg_encrypt /bin/ \
 && cp -rf shared /usr/share/QDK \
 && mkdir -p /etc/config \
 && sed -e '2d' shared/qdk.conf > /etc/config/qdk.conf \
 && sed -i '2iQDK_PATH_P=/usr/share' /etc/config/qdk.conf
ENV PATH=/usr/share/QDK/bin:$PATH
WORKDIR /work
