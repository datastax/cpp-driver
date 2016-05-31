require "formula"

class DseCppDriver < Formula
  homepage "http://datastax.github.io/cpp-driver-dse/"
  url "https://github.com/datastax/cpp-driver-dse/archive/1.0.0-eap1.tar.gz"
  sha1 "6159f1a5e31a044fb70567f849286572afa57174"
  version "1.0.0-eap1"

  head "git://github.com:datastax/cpp-driver-dse.git", :branch => "master"

  depends_on "cmake" => :build
  depends_on "libuv"

  def install
    mkdir 'build' do
      system "cmake", "-DCMAKE_BUILD_TYPE=RELEASE", "-DDSE_BUILD_STATIC=ON", "-DDSE_INSTALL_PKG_CONFIG=OFF", "-DCMAKE_INSTALL_PREFIX:PATH=#{prefix}", "-DCMAKE_INSTALL_LIBDIR=#{lib}", ".."
      system "make", "install"
    end
  end
end
