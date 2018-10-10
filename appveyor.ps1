<#
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#>

Function Get-Commit-Sha {
  return $($Env:APPVEYOR_REPO_COMMIT.SubString(0,7))
}

Function Get-OpenSSL-Version {
  $openssl_version = "$($(Get-ChildItem Env:"OPENSSL_$($Env:OPENSSL_MAJOR_MINOR.Replace(".", "_"))_VERSION").Value)"
  return $openssl_version
}

Function Bison-Version-Information {
  If (Get-Command "bison" -ErrorAction SilentlyContinue) {
    $temporary_file = New-TemporaryFile
    Start-Process -FilePath bison -ArgumentList "--version" -RedirectStandardOutput $($temporary_file) -Wait -NoNewWindow
    $output = Get-Content "$($temporary_file)" -Raw
    Write-Host "$($output.Trim())" -BackgroundColor DarkCyan
    Remove-Item $temporary_file
  } Else {
    Write-Host "Bison is not available" -BackgroundColor DarkRed
  }
}

Function Perl-Version-Information {
  If (Get-Command "perl" -ErrorAction SilentlyContinue) {
    $temporary_file = New-TemporaryFile
    Start-Process -FilePath perl -ArgumentList "--version" -RedirectStandardOutput $($temporary_file) -Wait -NoNewWindow
    $output = Get-Content "$($temporary_file)" -Raw
    Write-Host "$($output.Trim())" -BackgroundColor DarkGray
    Remove-Item $temporary_file
  } Else {
    Write-Host "Perl is not available" -BackgroundColor DarkRed
  }
}

Function Build-Configuration-Information {
  $output = @"
Visual Studio: $($Env:CMAKE_GENERATOR.Split(" ")[-2]) [$($Env:CMAKE_GENERATOR.Split(" ")[-1])]
Architecture:  $($Env:Platform)
Boost:         v$($Env:BOOST_VERSION)
libssh2:       v$($Env:LIBSSH2_VERSION)
libuv:         v$($Env:LIBUV_VERSION)
OpenSSL:       v$(Get-OpenSSL-Version)
Build Number:  $($Env:APPVEYOR_BUILD_NUMBER)
Branch:        $($Env:APPVEYOR_REPO_BRANCH)
SHA:           $(Get-Commit-Sha)
"@
  Write-Host "$($output)" -BackgroundColor DarkGreen
}

Function Hardware-Information {
  $computer_system = Get-CimInstance CIM_ComputerSystem
  $operating_system = Get-CimInstance CIM_OperatingSystem
  $processor = Get-CimInstance CIM_Processor
  $logical_disk = Get-CimInstance Win32_LogicalDisk -Filter "DeviceID = 'C:'"
  $capacity = "{0:N2}" -f ($logical_disk.Size / 1GB)
  $free_space = "{0:N2}" -f ($logical_disk.FreeSpace / 1GB)
  $free_space_percentage = "{0:P2}" -f ($logical_disk.FreeSpace / $logical_disk.Size)
  $ram = "{0:N2}" -f ($computer_system.TotalPhysicalMemory / 1GB)

  # Determine if hyper-threading is enabled in order to display number of cores
  $number_of_cores = "$($processor.NumberOfCores)"
  If ($processor.NumberOfCores -lt $processor.NumberOfLogicalProcessors) {
    $number_of_cores = "$($processor.NumberOfLogicalProcessors) (Hyper-Threading)"
  }

  $hardware_information = @"
Hardware Information for $($computer_system.Name):
  Operating System: $($operating_system.caption) (Version: $($operating_system.Version))
  CPU: $($processor.Name)
  Number of Cores: $($number_of_cores)
  HDD Capacity: $($capacity) GB
  HDD Free Capacity: $($free_space_percentage) ($($free_space) GB)
  RAM: $($ram) GB
"@
  Write-Host "$($hardware_information)" -BackgroundColor DarkMagenta
}

Function Environment-Information {
  Write-Host "Visual Studio Environment Variables:" -BackgroundColor DarkMagenta
  Get-ChildItem Env:VS* | ForEach-Object {
    Write-Host "  $($_.Name) = $($_.Value)" -BackgroundColor DarkMagenta
  }
}

Function Initialize-Build-Environment {
  # Get the versions for the third party dependencies
  $boost_version = $Env:BOOST_VERSION
  $libssh2_version = $Env:LIBSSH2_VERSION
  $libuv_version = $Env:LIBUV_VERSION
  $openssl_version = Get-OpenSSL-Version
  $Env:OPENSSL_VERSION = $openssl_version
  $kerberos_version = "4.1"
  $bison_version = "2.4.1"
  $perl_version = "5.26.2.1"

  # Determine the platform and create associate environment variables
  $architecture = "32"
  If ($Env:Platform -Like "x64") {
    $architecture = "64"
  }
  $lib_architecture = "lib$($architecture)"
  $windows_architecture = "win$($architecture)"

  # Determine which header file to use for determine driver version
  $driver_header_file = "cassandra.h"
  If ($Env:DRIVER_TYPE -Like "dse") {
    $driver_header_file = "dse.h"
  }

  # Get the driver version number from the header file
  $version = @()
  Get-Content "$($Env:APPVEYOR_BUILD_FOLDER)/include/$($driver_header_file)" | ForEach-Object {
    If ($_ -Match "#define .*_VERSION_.*") {
        $token = $_.Split(" ")[-1].Replace("`"", "")
        If ($token) {
          $version += , $token
        }
    }
  }
  $Env:DRIVER_VERSION = "$($version[0]).$($version[1]).$($version[2])"
  If ($version.Length -eq 4) {
    $Env:DRIVER_VERSION += "-$($version[3])"
  }

  # Generate the variables for use with third party dependencies
  $bin_location_prefix = "C:/projects/dependencies/bin/"
  $libs_location_prefix = "C:/projects/dependencies/libs/"
  $dependencies_location_prefix = "$($libs_location_prefix)/$($Env:Platform)/$($Env:VISUAL_STUDIO_INTERNAL_VERSION)/"
  $download_url_prefix = "https://raw.githubusercontent.com/mikefero/cpp-driver-msvc-libs/master"

  # Generate the environment variables for use with the CMake FindXXX scripts
  $Env:LIBUV_ROOT_DIR = "$($dependencies_location_prefix)/libuv-$($libuv_version)"
  $Env:OPENSSL_BASE_DIR = "$($dependencies_location_prefix)/openssl-$($openssl_version)"
  $Env:OPENSSL_ROOT_DIR = "$($Env:OPENSSL_BASE_DIR)/shared"
  $Env:DRIVER_INSTALL_DIR = "C:/projects/driver/lib"
  $Env:DRIVER_ARTIFACTS_DIR = "C:/projects/driver/artifacts"
  $Env:DRIVER_ARTIFACTS_LOGS_DIR = "$($Env:DRIVER_ARTIFACTS_DIR)/logs"

  # Generate the environment variables for the third party archives
  $Env:LIBUV_ARTIFACT_ARCHIVE = "libuv-$($libuv_version)-win$($architecture)-msvc$($Env:VISUAL_STUDIO_INTERNAL_VERSION).zip"
  $Env:OPENSSL_ARTIFACT_ARCHIVE = "openssl-$($openssl_version)-win$($architecture)-msvc$($Env:VISUAL_STUDIO_INTERNAL_VERSION).zip"

  # Generate DataStax Enterprise specific environment variables
  If ($Env:DRIVER_TYPE -Like "dse") {
    # Generate the Kerberos environment variables
    $Env:KERBEROS_ARCHIVE = "kfw-$($kerberos_version)-$($windows_architecture)-msvc100.zip"
    $Env:KERBEROS_DOWNLOAD_URL = "$($download_url_prefix)/kerberos/$($kerberos_version)/$($Env:KERBEROS_ARCHIVE)"
    $Env:KERBEROS_ROOT_DIR = "$($libs_location_prefix)/$($Env:Platform)/100/kfw-$($kerberos_version)"
  }

  # Generate default environment variables for per commit builds
  $boost_version_filename = "$($boost_version.Replace(".", "_"))"
  $visual_studio_version = "$($Env:VISUAL_STUDIO_INTERNAL_VERSION.Insert(2, "."))"

  # Generate the default Boost environment variables
  $Env:BOOST_ROOT = "C:/Libraries/boost_$($boost_version_filename)"
  $Env:BOOST_LIBRARYDIR = "$($Env:BOOST_ROOT)/$($lib_architecture)-msvc-$($visual_studio_version)"
  If (-Not (Test-Path -Path "$($Env:BOOST_LIBRARYDIR)")) {
    # Update the Boost environment variables for CMake to find installation
    $Env:BOOST_ROOT = "$($libs_location_prefix)/boost-$($boost_version)"
    $Env:BOOST_LIBRARYDIR = "$($Env:BOOST_ROOT)/$($lib_architecture)-msvc-$($visual_studio_version)"
  }
  $Env:BOOST_INCLUDEDIR = "$($Env:BOOST_ROOT)/include"

  # Generate the default libssh2 environment variables
  $Env:LIBSSH2_ROOT_DIR = "$($dependencies_location_prefix)/libssh2-$($libssh2_version)"

  # Generate the archive name for the driver test and examples artifacts
  $build_version = "$($Env:APPVEYOR_BUILD_NUMBER)-$($Env:APPVEYOR_REPO_BRANCH)"
  # TODO: Re-enable OpenSSL version appending if multiple OpenSSL versions are enabled
  #$Env:DRIVER_ARTIFACT_EXAMPLES_ARCHIVE = "$($Env:DRIVER_TYPE.ToLower())-cpp-driver-$($Env:DRIVER_VERSION)-examples-openssl-$($Env:OPENSSL_MAJOR_MINOR)-win$($architecture)-msvc$($Env:VISUAL_STUDIO_INTERNAL_VERSION).zip"
  #$Env:DRIVER_ARTIFACT_TESTS_ARCHIVE = "$($Env:DRIVER_TYPE.ToLower())-cpp-driver-$($Env:DRIVER_VERSION)-tests-openssl-$($Env:OPENSSL_MAJOR_MINOR)-win$($architecture)-msvc$($Env:VISUAL_STUDIO_INTERNAL_VERSION).zip"
  $Env:DRIVER_ARTIFACT_EXAMPLES_ARCHIVE = "$($Env:DRIVER_TYPE.ToLower())-cpp-driver-$($Env:DRIVER_VERSION)-examples-win$($architecture)-msvc$($Env:VISUAL_STUDIO_INTERNAL_VERSION).zip"
  $Env:DRIVER_ARTIFACT_TESTS_ARCHIVE = "$($Env:DRIVER_TYPE.ToLower())-cpp-driver-$($Env:DRIVER_VERSION)-tests-win$($architecture)-msvc$($Env:VISUAL_STUDIO_INTERNAL_VERSION).zip"

  # Generate the archive name for the driver packaging
  # TODO: Re-enable OpenSSL version appending if multiple OpenSSL versions are enabled
  #$Env:DRIVER_ARTIFACT_ARCHIVE = "$($Env:DRIVER_TYPE.ToLower())-cpp-driver-$($Env:DRIVER_VERSION)-openssl-$($Env:OPENSSL_MAJOR_MINOR)-win$($architecture)-msvc$($Env:VISUAL_STUDIO_INTERNAL_VERSION).zip"
  $Env:DRIVER_ARTIFACT_ARCHIVE = "$($Env:DRIVER_TYPE.ToLower())-cpp-driver-$($Env:DRIVER_VERSION)-win$($architecture)-msvc$($Env:VISUAL_STUDIO_INTERNAL_VERSION).zip"

  # Generate additional download/install environments for third party build requirements
  $Env:BISON_BINARIES_ARCHIVE = "bison-$($bison_version)-bin.zip"
  $Env:BISON_BINARIES_DOWNLOAD_URL = "http://downloads.sourceforge.net/gnuwin32/$($Env:BISON_BINARIES_ARCHIVE)"
  $Env:BISON_DEPENDENCIES_ARCHIVE = "bison-$($bison_version)-dep.zip"
  $Env:BISON_DEPENDENCIES_DOWNLOAD_URL = "http://downloads.sourceforge.net/gnuwin32/$($Env:BISON_DEPENDENCIES_ARCHIVE)"
  $Env:BISON_ROOT_DIR = "$($bin_location_prefix)/bison-$($bison_version)"
  $Env:PERL_STANDALONE_ARCHIVE = "strawberry-perl-$($perl_version)-64bit-portable.zip"
  $Env:PERL_STANDALONE_DOWNLOAD_URL = "http://strawberryperl.com/download/$($perl_version)/$($Env:PERL_STANDALONE_ARCHIVE)"
  $Env:PERL_ROOT_DIR = "$($bin_location_prefix)/perl-$($perl_version)"

  # Update the PATH to include the third party build requirement tools (prepend)
  $Env:PATH = "$($Env:BISON_ROOT_DIR)/bin;$($Env:PERL_ROOT_DIR)\perl\site\bin;$($Env:PERL_ROOT_DIR)\perl\bin;$($Env:PERL_ROOT_DIR)\c\bin;$($Env:KERBEROS_ROOT_DIR)/bin;$($Env:LIBUV_ROOT_DIR)/bin;$($Env:OPENSSL_ROOT_DIR)/bin;$($Env:PATH)"
}

Function Install-Driver-Environment {
  # Remove pre-installed OpenSSL (resolve conflicts)
  Remove-Item -Force -Recurse -Path "C:/OpenSSL-*"

  # Determine if Bison needs to be installed (cached)
  If (-Not (Test-Path -Path "$($Env:BISON_ROOT_DIR)")) {
    # Download and extract the dependency
    try {
      Write-Host "Downloading and extracting Bison for Windows"
      New-Item -ItemType Directory -Force -Path "$($Env:BISON_ROOT_DIR)" | Out-Null
      $is_download_complete = $False
      $retries = 0
      do {
        try {
          Invoke-WebRequest -Uri "$($Env:BISON_BINARIES_DOWNLOAD_URL)" -OutFile $Env:BISON_BINARIES_ARCHIVE -UserAgent [Microsoft.PowerShell.Commands.PSUserAgent]::Chrome
          $is_download_complete = $True
        } catch {
          Write-Host "Error downloading Bison binaries; sleeping for 10 seconds ... " -NoNewLine -BackgroundColor DarkRed
          Start-Sleep -s 10
          Write-Host "done." -BackgroundColor DarkRed
          Write-Host "Retrying Bison binaries download"
        }
        $retries++
      } while($is_download_complete -eq $False -and $retries -lt 10)
      $argument_list = @"
-o"$($Env:BISON_ROOT_DIR)" x "$($Env:BISON_BINARIES_ARCHIVE)"
"@
      $process = Start-Process -FilePath 7z -ArgumentList $argument_list -PassThru -Wait -NoNewWindow
      If ($process.ExitCode -ne 0) {
        Remove-Item -Force -Recurse -Path "$($Env:BISON_ROOT_DIR)"
        Throw "Failed to extract Bison binaries"
      }
      $is_download_complete = $False
      $retries = 0
      do {
        try {
          Invoke-WebRequest -Uri "$($Env:BISON_DEPENDENCIES_DOWNLOAD_URL)" -OutFile $Env:BISON_DEPENDENCIES_ARCHIVE -UserAgent [Microsoft.PowerShell.Commands.PSUserAgent]::Chrome
          $is_download_complete = $True
        } catch {
          Write-Host "Error downloading Bison dependencies; sleeping for 10 seconds ... " -NoNewLine -BackgroundColor DarkRed
          Start-Sleep -s 10
          Write-Host "done." -BackgroundColor DarkRed
          Write-Host "Retrying Bison dependencies download"
        }
        $retries++
      } while($is_download_complete -eq $False -and $retries -lt 10)
      $argument_list = @"
-aoa -o"$($Env:BISON_ROOT_DIR)" x "$($Env:BISON_DEPENDENCIES_ARCHIVE)"
"@
      $process = Start-Process -FilePath 7z -ArgumentList $argument_list -PassThru -Wait -NoNewWindow
      If ($process.ExitCode -ne 0) {
        Remove-Item -Force -Recurse -Path "$($Env:BISON_ROOT_DIR)"
        Throw "Failed to extract Bison dependencies"
      }

      # Delete the binary archive
      Remove-Item $Env:BISON_BINARIES_ARCHIVE
      Remove-Item $Env:BISON_DEPENDENCIES_ARCHIVE
    } catch {
      Remove-Item -Force -Recurse -Path "$($Env:BISON_ROOT_DIR)"
      Throw $PSItem
    }
  }

  # Display the Bison version information
  Bison-Version-Information

  # Determine if Strawberry Perl needs to be installed (cached)
  If (-Not (Test-Path -Path "$($Env:PERL_ROOT_DIR)")) {
    # Download and extract the dependency
    try {
      Write-Host "Downloading and extracting Strawberry Perl for Windows"
      New-Item -ItemType Directory -Force -Path "$($Env:PERL_ROOT_DIR)" | Out-Null
      If ($Env:APPVEYOR -Like "True") {
        Start-FileDownload "$($Env:PERL_STANDALONE_DOWNLOAD_URL)" -FileName $Env:PERL_STANDALONE_ARCHIVE
      } Else {
        curl.exe -o "$($Env:PERL_STANDALONE_ARCHIVE)" "$($Env:PERL_STANDALONE_DOWNLOAD_URL)"
      }
      $argument_list = @"
-o"$($Env:PERL_ROOT_DIR)" x "$($Env:PERL_STANDALONE_ARCHIVE)"
"@
      $process = Start-Process -FilePath 7z -ArgumentList $argument_list -PassThru -Wait -NoNewWindow
      If ($process.ExitCode -ne 0) {
        Remove-Item -Force -Recurse -Path "$($Env:PERL_ROOT_DIR)"
        Throw "Failed to extract Strawberry Perl"
      }

      # Delete the binary archive
      Remove-Item $Env:PERL_STANDALONE_ARCHIVE
    } catch {
      Remove-Item -Force -Recurse -Path "$($Env:PERL_ROOT_DIR)"
      Throw $PSItem
    }
  }

  # Display the Perl version information
  Perl-Version-Information

  # Determine the location of the CMake modules (external projects)
  $cmake_modules_dir = "$($Env:APPVEYOR_BUILD_FOLDER -Replace `"\\`", `"/`")/"
  If ($Env:DRIVER_TYPE -Like "dse") {
    $cmake_modules_dir += "cpp-driver/"
  }
  $cmake_modules_dir += "cmake/modules"

  # Determine the CMake generator to utilize
  $cmake_generator = $Env:CMAKE_GENERATOR
  If ($Env:Platform -Like "x64") {
    $cmake_generator += " Win64"
  }

  # Build and install the dependencies (if needed; cached)
  $dependencies_build_location_prefix = "C:/projects/dependencies/build/"
  If (-Not (Test-Path -Path "$($Env:LIBUV_ROOT_DIR)/lib")) { # lib directory checked due to external project being CMake (automatically creates root directory)
    New-Item -ItemType Directory -Force -Path "$($dependencies_build_location_prefix)/libuv" | Out-Null
    Push-Location -Path "$($dependencies_build_location_prefix)/libuv"

    $cmakelists_contents = @"
cmake_minimum_required(VERSION 2.8.12 FATAL_ERROR)
project(libuv)
set(PROJECT_DISPLAY_NAME "AppVeyor CI Build for libuv")
set(PROJECT_MODULE_DIR $cmake_modules_dir)
set(CMAKE_MODULE_PATH `${CMAKE_MODULE_PATH} `${PROJECT_MODULE_DIR})
include(ExternalProject-libuv)
set(GENERATED_SOURCE_FILE `${CMAKE_CURRENT_BINARY_DIR}/main.cpp)
file(REMOVE `${GENERATED_SOURCE_FILE})
file(WRITE `${GENERATED_SOURCE_FILE} "int main () { return 0; }")
add_executable(`${PROJECT_NAME} `${GENERATED_SOURCE_FILE})
add_dependencies(`${PROJECT_NAME} `${LIBUV_LIBRARY_NAME})
"@
    $cmakelists_contents | Out-File -FilePath "CMakeLists.txt" -Encoding Utf8 -Force

    Write-Host "Configuring libuv"
    cmake -G "$($cmake_generator)" -DBUILD_SHARED_LIBS=On "-DLIBUV_VERSION=$($Env:LIBUV_VERSION)" "-DLIBUV_INSTALL_PREFIX=$($Env:LIBUV_ROOT_DIR)"
    If ($LastExitCode -ne 0) {
      If (Test-Path -Path "build/CMakeFiles/CMakeOutput.log") {
        Push-AppveyorArtifact "build/CMakeFiles/CMakeOutput.log" -DeploymentName "libuv Output Log"
      }
      If (Test-Path -Path "build/CMakeFiles/CMakeError.log") {
        Push-AppveyorArtifact "build/CMakeFiles/CMakeError.log" -DeploymentName "libuv Error Log"
      }
      Pop-Location
      Throw "Failed to configure libuv for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
    }
    Write-Host "Building and Installing libuv"
    cmake --build . --config RelWithDebInfo
    If ($LastExitCode -ne 0) {
      If (Test-Path -Path "build/CMakeFiles/CMakeOutput.log") {
        Push-AppveyorArtifact "build/CMakeFiles/CMakeOutput.log" -DeploymentName "libuv Output Log"
      }
      If (Test-Path -Path "build/CMakeFiles/CMakeError.log") {
        Push-AppveyorArtifact "build/CMakeFiles/CMakeError.log" -DeploymentName "libuv Error Log"
      }
      Pop-Location
      Throw "Failed to build libuv for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
    }

    Pop-Location
  }

  $library_types = ("shared", "static")
  $library_types | foreach {
    If (-Not (Test-Path -Path "$($Env:OPENSSL_BASE_DIR)/$_")) {
      New-Item -ItemType Directory -Force -Path "$($dependencies_build_location_prefix)/openssl/$_" | Out-Null
      Push-Location -Path "$($dependencies_build_location_prefix)/openssl/$_"

      $cmakelists_contents = @"
cmake_minimum_required(VERSION 2.8.12 FATAL_ERROR)
project(OpenSSL)
set(PROJECT_DISPLAY_NAME "AppVeyor CI Build for OpenSSL")
set(PROJECT_MODULE_DIR $cmake_modules_dir)
set(CMAKE_MODULE_PATH `${CMAKE_MODULE_PATH} `${PROJECT_MODULE_DIR})
include(ExternalProject-OpenSSL)
set(GENERATED_SOURCE_FILE `${CMAKE_CURRENT_BINARY_DIR}/main.cpp)
file(REMOVE `${GENERATED_SOURCE_FILE})
file(WRITE `${GENERATED_SOURCE_FILE} "int main () { return 0; }")
add_executable(`${PROJECT_NAME} `${GENERATED_SOURCE_FILE})
add_dependencies(`${PROJECT_NAME} `${OPENSSL_LIBRARY_NAME})
"@
      $cmakelists_contents | Out-File -FilePath "CMakeLists.txt" -Encoding Utf8 -Force

      Write-Host "Configuring OpenSSL [$_]"
      $shared_libs = "Off"
      if ("$_" -Like "shared") {
        $shared_libs = "On"
      }
      cmake -G "$($cmake_generator)" "-DBUILD_SHARED_LIBS=$($shared_libs)" "-DOPENSSL_VERSION=$($Env:OPENSSL_VERSION)" "-DOPENSSL_INSTALL_PREFIX=$($Env:OPENSSL_BASE_DIR)/$_"
      If ($LastExitCode -ne 0) {
        If (Test-Path -Path "build/CMakeFiles/CMakeOutput.log") {
          Push-AppveyorArtifact "build/CMakeFiles/CMakeOutput.log" -DeploymentName "OpenSSL Output Log"
        }
        If (Test-Path -Path "build/CMakeFiles/CMakeError.log") {
          Push-AppveyorArtifact "build/CMakeFiles/CMakeError.log" -DeploymentName "OpenSSL Error Log"
        }
        Pop-Location
        Throw "Failed to configure OpenSSL for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
      }
      Write-Host "Building and Installing OpenSSL [$_]"
      cmake --build . --config RelWithDebInfo
      If ($LastExitCode -ne 0) {
        If (Test-Path -Path "build/CMakeFiles/CMakeOutput.log") {
          Push-AppveyorArtifact "build/CMakeFiles/CMakeOutput.log" -DeploymentName "OpenSSL Output Log"
        }
        If (Test-Path -Path "build/CMakeFiles/CMakeError.log") {
          Push-AppveyorArtifact "build/CMakeFiles/CMakeError.log" -DeploymentName "OpenSSL Error Log"
        }
        Pop-Location
        Throw "Failed to build OpenSSL for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
      }

      Pop-Location
    }
  }

  # Handle installation of DataStax Enterprise dependencies
  If ($Env:DRIVER_TYPE -Like "dse") {
    # Determine if Kerberos for Windows should be installed (cached)
    If (-Not (Test-Path -Path "$($Env:KERBEROS_ROOT_DIR)")) {
      # Download and extract the dependency
      try {
        Write-Host "Downloading and extracting Kerberos for Windows"
        New-Item -ItemType Directory -Force -Path "$($Env:KERBEROS_ROOT_DIR)" | Out-Null
        If ($Env:APPVEYOR -Like "True") {
          Start-FileDownload "$($Env:KERBEROS_DOWNLOAD_URL)" -FileName $Env:KERBEROS_ARCHIVE
        } Else {
          curl.exe -o "$($Env:KERBEROS_ARCHIVE)" "$($Env:KERBEROS_DOWNLOAD_URL)"
        }
        $argument_list = @"
-o"$($Env:KERBEROS_ROOT_DIR)" x "$($Env:KERBEROS_ARCHIVE)"
"@
        $process = Start-Process -FilePath 7z -ArgumentList $argument_list -PassThru -Wait -NoNewWindow
        If ($process.ExitCode -ne 0) {
          Remove-Item -Force -Recurse -Path "$($Env:KERBEROS_ROOT_DIR)"
          Throw "Failed to extract Kerberos"
        }

        # Delete the binary archive
        Remove-Item $Env:KERBEROS_ARCHIVE
      } catch {
        Remove-Item -Force -Recurse -Path "$($Env:KERBEROS_ROOT_DIR)"
        Throw $PSItem
      }
    }
  }

  # Handle installation of the per commit dependencies (e.g. Test builds)
  # Determine if Boost should be installed (pre-installed or cached)
  If (-Not (Test-Path -Path "$($Env:BOOST_LIBRARYDIR)")) {
    New-Item -ItemType Directory -Force -Path "$($dependencies_build_location_prefix)/boost" | Out-Null
    Push-Location -Path "$($dependencies_build_location_prefix)/boost"

    $cmakelists_contents = @"
cmake_minimum_required(VERSION 2.8.12 FATAL_ERROR)
project(Boost)
set(PROJECT_DISPLAY_NAME "AppVeyor CI Build for Boost")
set(PROJECT_MODULE_DIR $cmake_modules_dir)
set(CMAKE_MODULE_PATH `${CMAKE_MODULE_PATH} `${PROJECT_MODULE_DIR})
include(ExternalProject-boost)
set(GENERATED_SOURCE_FILE `${CMAKE_CURRENT_BINARY_DIR}/main.cpp)
file(REMOVE `${GENERATED_SOURCE_FILE})
file(WRITE `${GENERATED_SOURCE_FILE} "int main () { return 0; }")
add_executable(`${PROJECT_NAME} `${GENERATED_SOURCE_FILE})
add_dependencies(`${PROJECT_NAME} `${BOOST_LIBRARY_NAME})
"@
    $cmakelists_contents | Out-File -FilePath "CMakeLists.txt" -Encoding Utf8 -Force

    Write-Host "Configuring Boost"
    cmake -G "$($cmake_generator)" "-DBOOST_VERSION=$($Env:BOOST_VERSION)" "-DBOOST_INSTALL_PREFIX=$($Env:BOOST_ROOT)"
    If ($LastExitCode -ne 0) {
      If (Test-Path -Path "build/CMakeFiles/CMakeOutput.log") {
        Push-AppveyorArtifact "build/CMakeFiles/CMakeOutput.log" -DeploymentName "Boost Output Log"
      }
      If (Test-Path -Path "build/CMakeFiles/CMakeError.log") {
        Push-AppveyorArtifact "build/CMakeFiles/CMakeError.log" -DeploymentName "Boost Error Log"
      }
      Pop-Location
      Throw "Failed to configure Boost for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
    }
    Write-Host "Building and Installing Boost"
    cmake --build . --config RelWithDebInfo
    If ($LastExitCode -ne 0) {
      If (Test-Path -Path "build/CMakeFiles/CMakeOutput.log") {
        Push-AppveyorArtifact "build/CMakeFiles/CMakeOutput.log" -DeploymentName "Boost Output Log"
      }
      If (Test-Path -Path "build/CMakeFiles/CMakeError.log") {
        Push-AppveyorArtifact "build/CMakeFiles/CMakeError.log" -DeploymentName "Boost Error Log"
      }
      Pop-Location
      Throw "Failed to build Boost for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
    }

    Pop-Location
  }

  # Determine if libssh2 should be installed (cached)
  If (-Not (Test-Path -Path "$($Env:LIBSSH2_ROOT_DIR)/lib")) { # lib directory checked due to external project being CMake (automatically creates root directory)
    New-Item -ItemType Directory -Force -Path "$($dependencies_build_location_prefix)/libssh2" | Out-Null
    Push-Location -Path "$($dependencies_build_location_prefix)/libssh2"

    $cmakelists_contents = @"
cmake_minimum_required(VERSION 2.8.12 FATAL_ERROR)
project(libssh2)
set(PROJECT_DISPLAY_NAME "AppVeyor CI Build for libssh2")
set(PROJECT_MODULE_DIR $cmake_modules_dir)
set(CMAKE_MODULE_PATH `${CMAKE_MODULE_PATH} `${PROJECT_MODULE_DIR})
include(ExternalProject-libssh2)
set(GENERATED_SOURCE_FILE `${CMAKE_CURRENT_BINARY_DIR}/main.cpp)
file(REMOVE `${GENERATED_SOURCE_FILE})
file(WRITE `${GENERATED_SOURCE_FILE} "int main () { return 0; }")
add_executable(`${PROJECT_NAME} `${GENERATED_SOURCE_FILE})
add_dependencies(`${PROJECT_NAME} `${LIBSSH2_LIBRARY_NAME})
"@
    $cmakelists_contents | Out-File -FilePath "CMakeLists.txt" -Encoding Utf8 -Force

    Write-Host "Configuring libssh2"
    cmake -G "$($cmake_generator)" "-DLIBSSH2_VERSION=$($Env:LIBSSH2_VERSION)" "-DLIBSSH2_INSTALL_PREFIX=$($Env:LIBSSH2_ROOT_DIR)"
    If ($LastExitCode -ne 0) {
      If (Test-Path -Path "build/CMakeFiles/CMakeOutput.log") {
        Push-AppveyorArtifact "build/CMakeFiles/CMakeOutput.log" -DeploymentName "libssh2 Output Log"
      }
      If (Test-Path -Path "build/CMakeFiles/CMakeError.log") {
        Push-AppveyorArtifact "build/CMakeFiles/CMakeError.log" -DeploymentName "libssh2 Error Log"
      }
      Pop-Location
      Throw "Failed to configure libssh2 for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
    }
    Write-Host "Building and Installing libssh2"
    cmake --build . --config RelWithDebInfo
    If ($LastExitCode -ne 0) {
      If (Test-Path -Path "build/CMakeFiles/CMakeOutput.log") {
        Push-AppveyorArtifact "build/CMakeFiles/CMakeOutput.log" -DeploymentName "libssh2 Output Log"
      }
      If (Test-Path -Path "build/CMakeFiles/CMakeError.log") {
        Push-AppveyorArtifact "build/CMakeFiles/CMakeError.log" -DeploymentName "libssh2 Error Log"
      }
      Pop-Location
      Throw "Failed to build libssh2 for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
    }

    Pop-Location
  }

  # Archive any dependency builds logs and perform cleanup
  Get-ChildItem -File -Filter "*.log" -Recurse -Path "C:/projects/dependencies/build" | ForEach-Object {
    If ($_.FullName.ToLower() -Match "-stamp" -And
        $_.Length -gt 0kb) {
      New-Item -ItemType Directory -Force -Path "$($Env:DRIVER_ARTIFACTS_LOGS_DIR)" | Out-Null
      Copy-Item -Force -Path "$($_.FullName)" "$($Env:DRIVER_ARTIFACTS_LOGS_DIR)"
    }
  }
}

Function Build-Driver {
  # Determine the CMake generator to utilize
  $cmake_generator = $Env:CMAKE_GENERATOR
  If ($Env:Platform -Like "x64") {
    $cmake_generator += " Win64"
  }

  # Ensure Boost atomic is used for Visual Studio 2010 (increased performance)
  $use_boost_atomic = "Off"
  If ($Env:VISUAL_STUDIO_INTERNAL_VERSION -Like "100" -Or
      ($Env:VISUAL_STUDIO_INTERNAL_VERSION -Like "110" -And $Env:Platform -Like "x86")) {
    $use_boost_atomic = "On" # Enable Boost atomic usage
  }

  # Build the driver
  $driver_type = "Apache Cassandra"
  If ($Env:DRIVER_TYPE -Like "dse") {
    $driver_type = "DSE"
  }
  New-Item -ItemType Directory -Force -Path "$($Env:APPVEYOR_BUILD_FOLDER)/build"
  Push-Location "$($Env:APPVEYOR_BUILD_FOLDER)/build"
  Write-Host "Configuring DataStax C/C++ $($driver_type) Driver"
  cmake -G "$($cmake_generator)" "-D$($Env:DRIVER_TYPE)_MULTICORE_COMPILATION=On" "-D$($Env:DRIVER_TYPE)_USE_OPENSSL=On" "-D$($Env:DRIVER_TYPE)_USE_BOOST_ATOMIC=$($use_boost_atomic)" "-D$($Env:DRIVER_TYPE)_BUILD_EXAMPLES=On" "-D$($Env:DRIVER_TYPE)_BUILD_TESTS=On" "-D$($Env:DRIVER_TYPE)_USE_LIBSSH2=On" "-DCMAKE_INSTALL_PREFIX=`"$($Env:DRIVER_INSTALL_DIR)`"" ..
  If ($LastExitCode -ne 0) {
    Pop-Location
    Throw "Failed to configure DataStax C/C++ $($driver_type) Driver for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
  }
  Write-Host "Building and Installing DataStax C/C++ $($driver_type) Driver"
  cmake --build . --config RelWithDebInfo --target install
  If ($LastExitCode -ne 0) {
    Pop-Location
    Throw "Failed to build DataStax C/C++ $($driver_type) Driver for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
  }
  Pop-Location

  # Copy the binary artifacts
  New-Item -ItemType Directory -Force -Path "$($Env:DRIVER_ARTIFACTS_DIR)/bin/examples" | Out-Null
  New-Item -ItemType Directory -Force -Path "$($Env:DRIVER_ARTIFACTS_DIR)/bin/tests" | Out-Null
  Get-ChildItem -File -Filter "*.exe" -Recurse -Path "$($Env:APPVEYOR_BUILD_FOLDER)/build" | ForEach-Object {
    If ($_.FullName.ToLower() -Match "relwithdebinfo") {
      $suffix="bin"
     If ($_.FullName.ToLower() -Match "examples") {
        $suffix+="/examples"
      } ElseIf ($_.FullName.ToLower() -Match "test.*exe") {
        $suffix+="/tests"
      }
      Copy-Item -Force -Path "$($_.FullName)" "$($Env:DRIVER_ARTIFACTS_DIR)/$($suffix)"
    }
  }
}

Function Execute-Driver-Unit-Tests {
  # Update the PATH for the test executables to run with output
  $Env:PATH = "$($Env:DRIVER_ARTIFACTS_DIR)\bin\tests;$($Env:PATH)"

  # Execute the Apache Cassandra unit tests
  $is_failure = $False
  cassandra-unit-tests.exe --gtest_output=xml:"$($Env:DRIVER_ARTIFACTS_DIR)\bin\tests\cassandra-unit-tests-gtest-results.xml"
  If ($LastExitCode -ne 0) {
    $is_failure = $True
  }

  If ($Env:DRIVER_TYPE -Like "dse") {
    # Execute the Apache Cassandra unit tests
    dse-unit-tests.exe --gtest_output=xml:"$($Env:DRIVER_ARTIFACTS_DIR)\bin\tests\dse-unit-tests-gtest-results.xml"
    If ($LastExitCode -ne 0) {
      $is_failure = $True
    }
  }

  # Check to see if there was a failure executing the tests
  If ($is_failure) {
    Throw "Error Executing Unit tests: Check tests tab or download from the artifacts"
  }
}

Function Push-Driver-Unit-Tests-Results {
  # Push the unit test(s) results
  If ($Env:APPVEYOR -Like "True") {
    $web_client = New-Object "System.Net.WebClient"
    Get-ChildItem -File -Filter "*.xml" -Path "$($Env:DRIVER_ARTIFACTS_DIR)/bin/tests" -Recurse | ForEach-Object {
      $web_client.UploadFile("https://ci.appveyor.com/api/testresults/junit/$($Env:APPVEYOR_JOB_ID)", (Resolve-Path "$($_.FullName)"))
    }
  }
}

Function Package-Artifacts {
  # Package the driver artifacts
  New-Item -ItemType Directory -Force -Path "$($Env:DRIVER_ARTIFACTS_DIR)" | Out-Null
  $argument_list = @"
a -tzip "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:DRIVER_ARTIFACT_ARCHIVE)" -r "$($Env:DRIVER_INSTALL_DIR)/*"
"@
  $process = Start-Process -FilePath 7z -ArgumentList $argument_list -PassThru -Wait -NoNewWindow
  If ($process.ExitCode -ne 0) {
    Throw "Failed to archive driver for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
  }

  # Package the driver example and test artifacts
  $argument_list = @"
a -tzip "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:DRIVER_ARTIFACT_EXAMPLES_ARCHIVE)" -r "$($Env:DRIVER_ARTIFACTS_DIR)/bin/examples/*"
"@
  $process = Start-Process -FilePath 7z -ArgumentList $argument_list -PassThru -Wait -NoNewWindow
  If ($process.ExitCode -ne 0) {
    Throw "Failed to archive driver examples for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
  }
  $argument_list = @"
a -tzip "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:DRIVER_ARTIFACT_TESTS_ARCHIVE)" -r "$($Env:DRIVER_ARTIFACTS_DIR)/bin/tests/*.exe"
"@
  $process = Start-Process -FilePath 7z -ArgumentList $argument_list -PassThru -Wait -NoNewWindow
  If ($process.ExitCode -ne 0) {
    Throw "Failed to archive driver tests for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
  }

  # Clean up the library dependency directories for libuv packaging
  New-Item -ItemType Directory -Force -Path "$($Env:DRIVER_ARTIFACTS_DIR)/libuv" | Out-Null
  Copy-Item -Force -Recurse -Path "$($Env:LIBUV_ROOT_DIR)/*" "$($Env:DRIVER_ARTIFACTS_DIR)/libuv" | Out-Null
  $argument_list = @"
a -tzip "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:LIBUV_ARTIFACT_ARCHIVE)" -r "$($Env:DRIVER_ARTIFACTS_DIR)/libuv/*"
"@
  $process = Start-Process -FilePath 7z -ArgumentList $argument_list -PassThru -Wait -NoNewWindow
  If ($process.ExitCode -ne 0) {
    Throw "Failed to archive libuv for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
  }

  # Clean up the library dependency directories for OpenSSL packaging
  New-Item -ItemType Directory -Force -Path "$($Env:DRIVER_ARTIFACTS_DIR)/openssl" | Out-Null
  Copy-Item -Force -Recurse -Path "$($Env:OPENSSL_BASE_DIR)/*" "$($Env:DRIVER_ARTIFACTS_DIR)/openssl" | Out-Null
  Move-Item -Force -Path "$($Env:DRIVER_ARTIFACTS_DIR)/openssl/static/LICENSE" "$($Env:DRIVER_ARTIFACTS_DIR)/openssl" | Out-Null
  Move-Item -Force -Path "$($Env:DRIVER_ARTIFACTS_DIR)/openssl/static/include" "$($Env:DRIVER_ARTIFACTS_DIR)/openssl" | Out-Null
  Move-Item -Force -Path "$($Env:DRIVER_ARTIFACTS_DIR)/openssl/static/openssl.cnf" "$($Env:DRIVER_ARTIFACTS_DIR)/openssl" | Out-Null
  If (Test-Path -Path "$($Env:DRIVER_ARTIFACTS_DIR)/openssl/static/openssl.cnf.dist") {
    Move-Item -Force -Path "$($Env:DRIVER_ARTIFACTS_DIR)/openssl/static/openssl.cnf.dist" "$($Env:DRIVER_ARTIFACTS_DIR)/openssl" | Out-Null
  }
  Remove-Item -Force -Recurse -Path "$($Env:DRIVER_ARTIFACTS_DIR)/openssl/shared/include" | Out-Null
  Remove-Item -Force -Path "$($Env:DRIVER_ARTIFACTS_DIR)/openssl/shared/LICENSE" | Out-Null
  Remove-Item -Force -Path "$($Env:DRIVER_ARTIFACTS_DIR)/openssl/shared/openssl.cnf" | Out-Null
  If (Test-Path -Path "$($Env:DRIVER_ARTIFACTS_DIR)/openssl/shared/openssl.cnf.dist") {
    Remove-Item -Force -Path "$($Env:DRIVER_ARTIFACTS_DIR)/openssl/shared/openssl.cnf.dist" | Out-Null
  }
  $argument_list = @"
a -tzip "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:OPENSSL_ARTIFACT_ARCHIVE)" -r "$($Env:DRIVER_ARTIFACTS_DIR)/openssl/*"
"@
  $process = Start-Process -FilePath 7z -ArgumentList $argument_list -PassThru -Wait -NoNewWindow
  If ($process.ExitCode -ne 0) {
    Throw "Failed to archive OpenSSL for MSVC $($Env:VISUAL_STUDIO_INTERNAL_VERSION)-$($Env:Platform)"
  }
}

Function Push-Artifacts {
  If ($Env:APPVEYOR -Like "True") {
    $driver_type = "Apache Cassandra"
    If ($Env:DRIVER_TYPE -Like "dse") {
      $driver_type = "DSE"
    }

    Push-AppveyorArtifact "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:DRIVER_ARTIFACT_ARCHIVE)" -DeploymentName "DataStax C/C++ $($driver_type) Driver"
    Push-AppveyorArtifact "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:DRIVER_ARTIFACT_EXAMPLES_ARCHIVE)" -DeploymentName "DataStax C/C++ $($driver_type) Driver Examples"
    Push-AppveyorArtifact "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:DRIVER_ARTIFACT_TESTS_ARCHIVE)" -DeploymentName "DataStax C/C++ $($driver_type) Driver Tests"
    Push-AppveyorArtifact "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:LIBUV_ARTIFACT_ARCHIVE)" -DeploymentName "libuv v$($Env:LIBUV_VERSION)"
    Push-AppveyorArtifact "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:OPENSSL_ARTIFACT_ARCHIVE)" -DeploymentName "OpenSSL v$($Env:OPENSSL_VERSION)"
  }
}

Function Publish-Artifact-To-Artifactory {
  Param (
    [Parameter(Mandatory=$True)] [String] $Uri,
    [Parameter(Mandatory=$True)] [String] $FilePath,
    [Parameter(Mandatory=$False)] [Int] $TimeoutSec = 480
  )

  # Create the credentials and checksum headers
  $username = "$($Env:ARTIFACTORY_USERNAME)"
  $password = ConvertTo-SecureString -String "$($Env:ARTIFACTORY_PASSWORD)" -AsPlainText -Force
  $credential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $username, $password
  $md5_hash = $(Get-FileHash "$($FilePath)" -Algorithm MD5).Hash
  $sha1_hash = $(Get-FileHash "$($FilePath)" -Algorithm SHA1).Hash
  $sha256_hash = $(Get-FileHash "$($FilePath)" -Algorithm SHA256).Hash
  $headers = @{
#    "X-Checksum-Deploy" = $True
    "X-Checksum-Md5" = $md5_hash
    "X-Checksum-Sha1" = $sha1_hash
    "X-Checksum-Sha256" = $sha256_hash
  }

  # Publish the artifacts to artifactory
  Try {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 # Ensure TLS v1.2 is enabled
    $result = Invoke-RestMethod -Headers $headers -Uri "$($Uri)" -Method Put -InFile "$($FilePath)" -Credential $credential -ContentType "multipart/form-data" -TimeoutSec $TimeoutSec
  } Catch {
    $error_code = $_.Exception.Response.StatusCode.value__
    $message = $_.Exception.Message
    $filename = Split-Path $FilePath -leaf
    If ($Env:APPVEYOR -Like "True") {
      Add-AppveyorMessage -Category Error -Message "Unable to Upload $($filename) [$($error_code)]" -Details "$($message)"
    } Else {
      Write-Error -Message "Unable to Upload $($filename) [$($error_code)]: $($message)"
    }
    return $error_code
  }
  return 0
}

Function Publish-Artifacts {
  # Determine if the artifacts should to published to Artifactory
  If ($Env:APPVEYOR_REPO_TAG -Like "True") {
    # Create the upload environment
    $driver_type = "cassandra"
    If ($Env:DRIVER_TYPE -Like "dse") {
      $driver_type = "dse"
    }

    # Create the Uri and FilePath components for the upload
    $base_uri = "$($Env:ARTIFACTORY_BASE_URI)/origin/$($Env:APPVEYOR_REPO_BRANCH)/$(Get-Commit-Sha)/windows"
    $driver_uri = "$($base_uri)/$($driver_type)/v$($Env:DRIVER_VERSION)/$($Env:DRIVER_ARTIFACT_ARCHIVE)"
    $driver_archive = "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:DRIVER_ARTIFACT_ARCHIVE)"
    $libuv_uri = "$($base_uri)/dependencies/libuv/v$($Env:LIBUV_VERSION)/$($Env:LIBUV_ARTIFACT_ARCHIVE)"
    $libuv_archive = "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:LIBUV_ARTIFACT_ARCHIVE)"
    #TODO: Need to handle OpenSSL v1.1.x if enabled
    $openssl_uri = "$($base_uri)/dependencies/openssl/v$($Env:OPENSSL_VERSION)/$($Env:OPENSSL_ARTIFACT_ARCHIVE)"
    $openssl_archive = "$($Env:DRIVER_ARTIFACTS_DIR)/$($Env:OPENSSL_ARTIFACT_ARCHIVE)"

    # Publish/Upload the driver and it dependencies to Artifactory
    $is_failure = $False
    $failed_upload = @()
    If ((Publish-Artifact-To-Artifactory -Uri "$($driver_uri)" -FilePath "$($driver_archive)") -ne 0) {
      $is_failure = $True
      $failed_upload += "Driver"
    }
    If ((Publish-Artifact-To-Artifactory -Uri "$($libuv_uri)" -FilePath "$($libuv_archive)") -ne 0) {
      $is_failure = $True
      $failed_upload += "libuv"
    }
    #TODO: Need to handle OpenSSL v1.1.x if enabled
    If ((Publish-Artifact-To-Artifactory -Uri "$($openssl_uri)" -FilePath "$($openssl_archive)") -ne 0) {
      $is_failure = $True
      $failed_upload += "OpenSSL"
    }

    # Check to see if there was a failure uploading the artifacts
    If ($is_failure) {
      Throw "Error Uploading Artifacts to Artifactory: $($failed_upload -Join ", ")"
    }
  }
}

Function Push-Build-Logs {
  If ($Env:APPVEYOR -Like "True") {
    # Determine the prefix directory based on driver type
    $prefix_dir = "./"
    If ($Env:DRIVER_TYPE -Like "dse") {
      $prefix_dir += "cpp-driver"
    }

    # Push the logs for the builds that occurred
    If (Test-Path -Path "$($Env:DRIVER_ARTIFACTS_LOGS_DIR)") {
      $argument_list = @"
a -tzip "cmake_external_project_logs.zip" -r "$($Env:DRIVER_ARTIFACTS_LOGS_DIR)/*.log"
"@
      $process = Start-Process -FilePath 7z -ArgumentList $argument_list -PassThru -Wait -NoNewWindow
      If ($process.ExitCode -eq 0) {
        Push-AppveyorArtifact "cmake_external_project_logs.zip" -DeploymentName "CMake External Project Logs"
      }
    }
    If (Test-Path -Path "$($Env:APPVEYOR_BUILD_FOLDER)/build/CMakeFiles/CMakeOutput.log") {
      Push-AppveyorArtifact "$($Env:APPVEYOR_BUILD_FOLDER)/build/CMakeFiles/CMakeOutput.log" -DeploymentName "Driver Output Log"
    }
    If (Test-Path -Path "$($Env:APPVEYOR_BUILD_FOLDER)/build/CMakeFiles/CMakeError.log") {
      Push-AppveyorArtifact "$($Env:APPVEYOR_BUILD_FOLDER)/build/CMakeFiles/CMakeError.log" -DeploymentName "Driver Error Log"
    }
  }
}
