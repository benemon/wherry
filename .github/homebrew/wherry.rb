class Wherry < Formula
  desc "Delegate coding agent tasks to receivers on other compute over Kafka"
  homepage "https://benemon.github.io/wherry/"
  license "MPL-2.0"

  on_macos do
    on_arm do
      url "https://github.com/benemon/wherry/releases/download/@TAG@/wherry_@TAG@_darwin_arm64.tar.gz"
      sha256 "@DARWIN_ARM64@"
    end
    on_intel do
      url "https://github.com/benemon/wherry/releases/download/@TAG@/wherry_@TAG@_darwin_amd64.tar.gz"
      sha256 "@DARWIN_AMD64@"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/benemon/wherry/releases/download/@TAG@/wherry_@TAG@_linux_arm64.tar.gz"
      sha256 "@LINUX_ARM64@"
    end
    on_intel do
      url "https://github.com/benemon/wherry/releases/download/@TAG@/wherry_@TAG@_linux_amd64.tar.gz"
      sha256 "@LINUX_AMD64@"
    end
  end

  def install
    bin.install "wherry"
  end

  test do
    assert_match "version=v#{version}", shell_output("#{bin}/wherry version")
  end
end
