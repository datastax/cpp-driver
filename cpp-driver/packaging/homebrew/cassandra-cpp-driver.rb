require "formula"

class CassandraCppDriver < Formula
  homepage "http://datastax.github.io/cpp-driver/"
  url "https://github.com/datastax/cpp-driver/archive/2.7.0.tar.gz"
  sha256 "44a97679e719b2b046ef90323beab4fe3a491ae79396e7f28e6a9677b618a0e4"
  version "2.7.0"

  head "git://github.com:datastax/cpp-driver.git", :branch => "master"

  depends_on "cmake" => :build
  depends_on "libuv"
  depends_on "openssl"

  def install
    mkdir 'build' do
      system "cmake", "-DCMAKE_BUILD_TYPE=RELEASE", "-DCASS_BUILD_STATIC=ON", "-DCASS_INSTALL_PKG_CONFIG=OFF", "-DCMAKE_INSTALL_PREFIX:PATH=#{prefix}", "-DCMAKE_INSTALL_LIBDIR=#{lib}", ".."
      system "make", "install"
    end
  end
end
