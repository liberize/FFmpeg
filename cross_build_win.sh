#!/bin/sh

MXE_PATH='/Users/liberize/Code/GitHub/mxe'
TARGET=i686-w64-mingw32.static
# change to 1 if build failed with error: *** write jobserver: Bad file descriptor
JOBS=1

export PATH="$MXE_PATH/usr/bin:$PATH"

# edit $MXE_PATH/src/ffmpeg.mk
# $(PKG)_DEPS add `curl`
# $(PKG)_BUILD add `--enable-libcurl`

# edit $MXE_PATH/src/ffmpeg.mk and $MXE_PATH/src/curl.mk
# $(PKG)_DEPS replace `gnutls` with `openssl`
# ffmpeg.mk: $(PKG)_BUILD replace `--enable-gnutls` with `--enable-openssl`; add `--enable-nonfree`
# curl.mk: $(PKG)_BUILD replace `--with-gnutls --without-ssl` with `--with-ssl`

FFMPEG_PATH="$(pwd)"
cd "$MXE_PATH"
make -j $JOBS MXE_TARGETS=$TARGET ffmpeg_SOURCE_TREE="$FFMPEG_PATH" ffmpeg
