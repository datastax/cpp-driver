require "formula"

class DatastaxCppDriver < Formula
  homepage "http://datastax.github.io/cpp-driver/"
  url "https://github.com/datastax/cpp-driver/archive/1.0.0.tar.gz"
  sha1 "b1dc34441ae83d1eb57ecd278090e5a627eb5c95"
  version "1.0.0"

  head "git://github.com:datastax/cpp-driver.git", :branch => "1.0"

  depends_on "cmake" => :build
  depends_on "libuv"

  def install
    mkdir 'build' do
      system "cmake", "-DCMAKE_BUILD_TYPE=RELEASE", "-DCASS_BUILD_STATIC=ON", "-DCMAKE_INSTALL_PREFIX:PATH=#{prefix}", ".."
      system "make", "install"
    end
  end
end
