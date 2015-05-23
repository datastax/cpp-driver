require "formula"

class CassandraCppDriver < Formula
  homepage "http://datastax.github.io/cpp-driver/"
  url "https://github.com/datastax/cpp-driver/archive/2.0.1.tar.gz"
  sha1 "f00de7ce2c6cc702426d443639d7bf9855fbe8a3"
  version "2.0.0"

  head "git://github.com:datastax/cpp-driver.git", :branch => "2.0"

  depends_on "cmake" => :build
  depends_on "libuv"

  def install
    mkdir 'build' do
      system "cmake", "-DCMAKE_BUILD_TYPE=RELEASE", "-DCASS_BUILD_STATIC=ON", "-DCMAKE_INSTALL_PREFIX:PATH=#{prefix}", ".."
      system "make", "install"
    end
  end
end
