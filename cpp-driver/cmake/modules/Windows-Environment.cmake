# Copyright (c) DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Define library package type
if(BUILD_SHARED_LIBS)
  set(PACKAGE_BUILD_TYPE "shared" CACHE STRING "Package build type" FORCE)
else()
  set(PACKAGE_BUILD_TYPE "static" CACHE STRING "Package build type" FORCE)
endif()

# Define package architecture type
if(CMAKE_CL_64)
  set(PACKAGE_ARCH_TYPE "win64" CACHE STRING "Package architecture type" FORCE)
  set(ARCH_TYPE "64" CACHE STRING "Architecture type" FORCE)
else()
  set(PACKAGE_ARCH_TYPE "win32" CACHE STRING "Package architecture type" FORCE)
  set(ARCH_TYPE "32" CACHE STRING "Architecture type" FORCE)
endif()

# Determine if Visual Studio is available
if(MSVC_VERSION)
  # Get the internal and toolset version and VC root directory
  if(CMAKE_VS_PLATFORM_TOOLSET)
    string(REPLACE "v" "" VS_INTERNAL_VERSION ${CMAKE_VS_PLATFORM_TOOLSET})
  endif()
  if(DEFINED ENV{APPVEYOR} AND DEFINED ENV{VISUAL_STUDIO_INTERNAL_VERSION})
    if (CMAKE_CL_64 AND
        ($ENV{VISUAL_STUDIO_INTERNAL_VERSION} EQUAL 100 OR
         $ENV{VISUAL_STUDIO_INTERNAL_VERSION} EQUAL 110))
      # Attempt to handle express/community editions (VS 2010) on AppVeyor
      set(VS_INTERNAL_VERSION $ENV{VISUAL_STUDIO_INTERNAL_VERSION})
    endif()
  endif()
  string(LENGTH ${VS_INTERNAL_VERSION} VS_INTERNAL_VERSION_LENGTH)
  math(EXPR VS_TOOLSET_MAJOR_VERSION_LENGTH "${VS_INTERNAL_VERSION_LENGTH} - 1")
  string(SUBSTRING ${VS_INTERNAL_VERSION} 0 ${VS_TOOLSET_MAJOR_VERSION_LENGTH} VS_TOOLSET_MAJOR_VERSION)
  string(SUBSTRING ${VS_INTERNAL_VERSION} ${VS_TOOLSET_MAJOR_VERSION_LENGTH} -1 VS_TOOLSET_MINOR_VERSION)
  set(VS_TOOLSET_VERSION "${VS_TOOLSET_MAJOR_VERSION}.${VS_TOOLSET_MINOR_VERSION}")

  # Get the command VC directories for environment scripts
  if(DEFINED ENV{APPVEYOR} AND DEFINED ENV{VISUAL_STUDIO_INTERNAL_VERSION})
    if($ENV{VISUAL_STUDIO_INTERNAL_VERSION} EQUAL 100 OR
       $ENV{VISUAL_STUDIO_INTERNAL_VERSION} EQUAL 110)
      # Attempt to handle express/community editions (VS 2010/2012) on AppVeyor
      set(CMAKE_VS_DEVENV_COMMAND "$ENV{ProgramFiles}/Microsoft Visual Studio ${VS_TOOLSET_VERSION}/Common7/IDE/devenv.exe")
    endif()
  endif()
  get_filename_component(DEVENV_DIR ${CMAKE_VS_DEVENV_COMMAND} DIRECTORY)
  get_filename_component(VS_VC_DIR ${DEVENV_DIR}/../../VC ABSOLUTE)
  get_filename_component(VS_TOOLS_DIR ${DEVENV_DIR}/../../Common7/Tools ABSOLUTE)
  get_filename_component(VS_BUILD_DIR ${VS_VC_DIR}/Auxiliary/Build ABSOLUTE)

  # Determine if a modern version of MSVC is being used
  if(VS_INTERNAL_VERSION EQUAL 141 OR VS_INTERNAL_VERSION GREATER 141) # Visual Studio 2017
    # Determine the target architecture for the Visual Studio environment
    if(CMAKE_CL_64)
      set(MSVC_ENVIRONMENT_SCRIPT "${VS_BUILD_DIR}/vcvarsx86_amd64.bat" CACHE STRING "Visual Studio environment script" FORCE)
    else()
      set(MSVC_ENVIRONMENT_SCRIPT "${VS_BUILD_DIR}/vcvars32.bat" CACHE STRING "Visual Studio environment script" FORCE)
    endif()
  else()
    # Determine if we are in a CI environment
    if(DEFINED ENV{APPVEYOR})
      set(MSVC_ENVIRONMENT_SCRIPT "${VS_VC_DIR}/vcvarsall.bat" CACHE STRING "Visual Studio environment script" FORCE)
      if(CMAKE_CL_64)
        set(MSVC_ENVIRONMENT_ARCH "x86_amd64" CACHE STRING "Visual Studio environment architecture" FORCE)
      else()
        set(MSVC_ENVIRONMENT_ARCH "x86" CACHE STRING "Visual Studio environment architecture" FORCE)
      endif()
    else()
      # Determine the target architecture for the Visual Studio environment
      if(CMAKE_CL_64)
        set(MSVC_ENVIRONMENT_SCRIPT "${VS_VC_DIR}/bin/x86_amd64/vcvarsx86_amd64.bat" CACHE STRING "Visual Studio environment script" FORCE)
      else()
        set(MSVC_ENVIRONMENT_SCRIPT "${VS_TOOLS_DIR}/vsvars32.bat" CACHE STRING "Visual Studio environment script" FORCE)
      endif()
    endif()
  endif()

  # Ensure the script exists
  if(NOT EXISTS ${MSVC_ENVIRONMENT_SCRIPT})
    unset(MSVC_ENVIRONMENT_SCRIPT)
    unset(VS_INTERNAL_VERSION)
    unset(VS_VC_DIR)
    unset(VS_BUILD_DIR)
    unset(VS_TOOLS_DIR)
    message(FATAL_ERROR "Unable to locate Visual Studio environment script")
  endif()

  # Ensure the Visual Studio environment script can be invoked by a batch script
  file(TO_NATIVE_PATH ${MSVC_ENVIRONMENT_SCRIPT} MSVC_ENVIRONMENT_SCRIPT)
endif()
