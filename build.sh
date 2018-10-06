#!/bin/sh

brew install automake fdk-aac git lame libass libtool libvorbis libvpx opus sdl shtool texi2html theora wget x264 x265 xvid nasm
brew install curl openssl sdl2
export PKG_CONFIG_PATH="/usr/local/opt/curl/lib/pkgconfig:/usr/local/opt/openssl/lib/pkgconfig:$PKG_CONFIG_PATH"
./configure --prefix=$(pwd)/inst --enable-shared --enable-pthreads --enable-gpl --enable-version3 --enable-hardcoded-tables --enable-nonfree --enable-libass --enable-libfdk-aac --enable-libfreetype --enable-libmp3lame --enable-libtheora --enable-libvorbis --enable-libvpx --enable-libx264 --enable-libx265 --enable-libopus --enable-libxvid --enable-opencl --enable-openssl --enable-libcurl
make -j8
