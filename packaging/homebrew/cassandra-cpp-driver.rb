require "formula"

class CassandraCppDriver < Formula
  homepage "http://datastax.github.io/cpp-driver/"
  url "https://github.com/datastax/cpp-driver/archive/2.2.0-beta1.tar.gz"
  sha256 "5a7325057231b65ad36359fe07d37b0dfbf2a522fa6961138c5efbaa7b265831"
  version "2.2.0-beta1"

  head "git://github.com:datastax/cpp-driver.git", :branch => "master"

  depends_on "cmake" => :build
  depends_on "libuv"
  depends_on "openssl"
  depends_on "boost160"

  def install
    mkdir 'build' do
      system "cmake", "-DCMAKE_BUILD_TYPE=RELEASE", "-DCASS_BUILD_STATIC=ON", "-DCASS_INSTALL_PKG_CONFIG=OFF", "-DCMAKE_INSTALL_PREFIX:PATH=#{prefix}", "-DCMAKE_INSTALL_LIBDIR=#{lib}",".."
      system "make", "install"
    end
  end
end
