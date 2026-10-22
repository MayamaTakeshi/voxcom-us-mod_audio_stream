#!/bin/bash
### Initial one liner:
# sudo apt-get -y install git \
#    && cd /usr/src/ \
#    && git clone https://github.com/voxcom-us/mod_audio_stream.git \
#    && cd mod_audio_stream \
#    && sudo bash ./build-mod-audio-stream.sh

apt-get -y install libfreeswitch-dev libssl-dev zlib1g-dev libevent-dev libspeexdsp-dev cmake

git submodule init
git submodule update

export PKG_CONFIG_PATH=/usr/local/freeswitch/lib/pkgconfig

# The module installs into the freeswitch.pc "modulesdir". Take it from the
# installed FreeSWITCH pkgconfig, not from the distro's libfreeswitch-dev
# (whose freeswitch.pc points at /usr/local/lib, the wrong place for
# /usr/local/freeswitch installs).
FS_MOD_DIR="$(pkg-config --variable=modulesdir freeswitch)"
if [ ! -d "${FS_MOD_DIR}" ] || [ "${FS_MOD_DIR}" != "/usr/local/freeswitch/mod" ]; then
  echo "ERROR: unexpected FreeSWITCH modules dir '${FS_MOD_DIR}' (pkg-config)" >&2
  exit 1
fi

mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_TLS=ON ..
make
make install
