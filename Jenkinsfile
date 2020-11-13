#!groovy
import org.jenkinsci.plugins.workflow.steps.FlowInterruptedException

def initializeEnvironment() {
  env.DRIVER_DISPLAY_NAME = 'Cassandra C/C++ Driver'
  env.DRIVER_TYPE = 'CASS'
  env.DRIVER_METRIC_TYPE = 'oss'
  env.DRIVER_BUILD_SCRIPT = '.build.sh'
  env.DRIVER_CASSANDRA_HEADER = 'include/cassandra.h'
  env.DRIVER_VERSION_HEADER = 'include/cassandra.h'
  env.DRIVER_LIBRARY = 'cassandra'
  if (env.GIT_URL.contains('cpp-driver-private')) {
    env.DRIVER_DISPLAY_NAME = 'private ' + env.DRIVER_DISPLAY_NAME
    env.DRIVER_METRIC_TYPE = 'oss-private'
  } else if (env.GIT_URL.contains('cpp-dse-driver')) {
    env.DRIVER_DISPLAY_NAME = 'DSE C/C++ Driver'
    env.DRIVER_TYPE = 'DSE'
    env.DRIVER_METRIC_TYPE = 'dse'
    env.DRIVER_BUILD_SCRIPT = "cpp-driver/${env.DRIVER_BUILD_SCRIPT}"
    env.DRIVER_CASSANDRA_HEADER = 'cpp-driver/include/cassandra.h'
    env.DRIVER_VERSION_HEADER = 'include/dse.h'
    env.DRIVER_LIBRARY = 'dse'
  }

  env.DRIVER_VERSION = sh(label: 'Determine driver version', script: '''#!/bin/bash -le
    . ${DRIVER_BUILD_SCRIPT}

    echo $(get_driver_version "${DRIVER_VERSION_HEADER}" "${DRIVER_TYPE}")''', returnStdout: true).trim()

  env.GIT_SHA = "${env.GIT_COMMIT.take(7)}"
  env.GITHUB_PROJECT_URL = "https://${GIT_URL.replaceFirst(/(git@|http:\/\/|https:\/\/)/, '').replace(':', '/').replace('.git', '')}"
  env.GITHUB_BRANCH_URL = "${GITHUB_PROJECT_URL}/tree/${env.BRANCH_NAME}"
  env.GITHUB_COMMIT_URL = "${GITHUB_PROJECT_URL}/commit/${env.GIT_COMMIT}"

  if (env.CI_INTEGRATION_ENABLED == 'true') {
    sh label: 'Download Apache Cassandra&reg; or DataStax Enterprise', script: '''#!/bin/bash -lex
      . ${CCM_ENVIRONMENT_SHELL} ${SERVER_VERSION}
    '''
  }

  sh label: 'Display C++ version and environment information', script: '''#!/bin/bash -le
    . ${DRIVER_BUILD_SCRIPT}

    # Load CCM environment variables
    if [ -f ${HOME}/environment.txt ]; then
      set -o allexport
      . ${HOME}/environment.txt
      set +o allexport
    fi

    if [ "${OS_DISTRO}" = "osx" ]; then
      clang --version
    else
      g++ --version
    fi
    cmake --version
    printenv | sort
  '''
}

def installDependencies() {
  sh label: 'Install dependencies', script: '''#!/bin/bash -le
    . ${DRIVER_BUILD_SCRIPT}

    install_dependencies
    if [ -d packaging/packages ]; then
      mkdir -p ${OS_DISTRO}/${OS_DISTRO_RELEASE}
      cp packaging/packages/libuv* ${OS_DISTRO}/${OS_DISTRO_RELEASE}
    fi
  '''
}

def buildDriver() {
  sh label: 'Build driver', script: '''#!/bin/bash -le
    . ${DRIVER_BUILD_SCRIPT}

    build_driver "${DRIVER_TYPE}"
    mkdir -p ${OS_DISTRO}/${OS_DISTRO_RELEASE}
    if [ -f build/cassandra-unit-tests ]; then
      cp build/cassandra-unit-tests ${OS_DISTRO}/${OS_DISTRO_RELEASE}
    fi
    if [ -f build/dse-unit-tests ]; then
      cp build/dse-unit-tests ${OS_DISTRO}/${OS_DISTRO_RELEASE}
    fi
    if [ -f build/cassandra-integration-tests ]; then
      cp build/cassandra-integration-tests ${OS_DISTRO}/${OS_DISTRO_RELEASE}
    fi
    if [ -f build/dse-integration-tests ]; then
      cp build/dse-integration-tests ${OS_DISTRO}/${OS_DISTRO_RELEASE}
    fi
    if [ -f build/CMakeFiles/CMakeOutput.log ]; then
      cp build/CMakeFiles/CMakeOutput.log ${OS_DISTRO}/${OS_DISTRO_RELEASE}
    fi
    if [ -f build//CMakeFiles/CMakeError.log ]; then
      cp build//CMakeFiles/CMakeError.log ${OS_DISTRO}/${OS_DISTRO_RELEASE}
    fi
  '''
}

def validateDriverExports() {
  sh label: 'Validate driver exports', script: '''#!/bin/bash -le
    . ${DRIVER_BUILD_SCRIPT}

    FUNCTIONS+=($(grep -Eoh '(^cass_\\s*(\\w+)\\s*\\()|(^dse_\\s*(\\w+)\\s*\\()' include/dse.h | awk -F '(' '{print $1}'));
    FUNCTIONS+=($(grep -Eoh '^cass_\\s*(\\w+)\\s*\\(' ${DRIVER_CASSANDRA_HEADER} | awk -F '(' '{print $1}'))
    check_driver_exports "${DRIVER_LIBRARY}" "${FUNCTIONS[@]}"
  '''
}

def configureTestingEnvironment() {
  sh label: 'Configure testing environment', script: '''#!/bin/bash -le
    . ${DRIVER_BUILD_SCRIPT}

    configure_testing_environment
  '''
}

def buildDocuments() {
  sh label: 'Building documents with documentor', script: '''#!/bin/bash -le
    documentor --output-directory ${HOME}/docs/drivers/cpp .
  '''

  sh label: 'Archive documentor generated documents', script: '''#!/bin/bash -le
    (
      cd ${HOME}/docs/drivers/cpp
      prefix=cpp-driver
      if [ "${DRIVER_TYPE}" = 'DSE' ]; then
        prefix=cpp-dse-driver
      fi
      tar czf ${WORKSPACE}/${prefix}-documents.tgz -C ${HOME}/docs/drivers/cpp .
    )
  '''
}

def executeUnitTests() {
  def attempt = 0
  retry(3) {
    try {
      timeout(time: 10, unit: 'MINUTES') {
        sh label: 'Execute unit tests', script: '''#!/bin/bash -le
          . ${DRIVER_BUILD_SCRIPT}

          build/cassandra-unit-tests --gtest_output=xml:cassandra-unit-tests-${OS_DISTRO}-${OS_DISTRO_RELEASE}-results.xml
          if [ -f build/dse-unit-tests ]; then
            build/dse-unit-tests --gtest_output=xml:dse-unit-tests-${OS_DISTRO}-${OS_DISTRO_RELEASE}-results.xml
          fi
        '''
      }
    } catch (FlowInterruptedException fie) {
      attempt++
      def message = "Retry attempt will be performed"
      if (attempt >= 3) {
        message = "No more retries will be performed"
      }
      // Work around https://issues.jenkins-ci.org/browse/JENKINS-51454
      error "Timeout Has Exceeded: ${message}"
    }
  }
}

def executeIntegrationTests() {
  timeout(time: 5, unit: 'HOURS') {
    sh label: 'Execute integration tests', script: '''#!/bin/bash -le
      . ${DRIVER_BUILD_SCRIPT}

      # Load CCM environment variables
      set -o allexport
      . ${HOME}/environment.txt
      set +o allexport

      # Disable exit on non 0
      set +e

      # Limit execution time on static nodes (no integration tests should be run on static node)
      if [ "${OS_VERSION}" != "osx/high-sierra" ]; then
        if [ -z "${TEST_FILTER}" ]; then
          TEST_FILTER=*
        fi
        # Limit DBAAS tests to Ubuntu 18.04
        if [ "${OS_VERSION}" != "ubuntu/bionic64/cpp" ]; then
          # Handle filters with negative filtering already applied
          if [[ "${TEST_FILTER}" == *":-"* ]]; then
            TEST_FILTER=${TEST_FILTER}:DbaasTests.*
          else
            TEST_FILTER=${TEST_FILTER}:-DbaasTests.*
          fi
        fi

        # Copy any JAR files to the current working directory
        cp build/*.jar .

        build/cassandra-integration-tests --${CCM_SERVER_TYPE} \
                                          --category=cassandra \
                                          --gtest_filter=${TEST_FILTER} \
                                          --gtest_output=xml:cassandra-integration-tests-${OS_DISTRO}-${OS_DISTRO_RELEASE}-results.xml \
                                          --version=${CCM_VERSION} \
                                          --verbose || cassandra_error=true
        if [ "${CCM_IS_DSE}" = "true" ]; then
          build/${DRIVER_LIBRARY}-integration-tests --${CCM_SERVER_TYPE} \
                                                    --category=dse \
                                                    --gtest_filter=${TEST_FILTER} \
                                                    --gtest_output=xml:dse-integration-tests-${OS_DISTRO}-${OS_DISTRO_RELEASE}-results.xml \
                                                    --version=${CCM_VERSION} \
                                                    --verbose || dse_error=true
        fi

        # Archive logs if an error occurred and ensure exit status is returned
        if [ ${cassandra_error} ] || [ ${dse_error} ]; then
          if [ -d log ]; then
            mkdir -p ${OS_DISTRO}/${OS_DISTRO_RELEASE}/${CCM_SERVER_TYPE}/${CCM_VERSION}
            (
              cd log
              tar czf ../${OS_DISTRO}/${OS_DISTRO_RELEASE}/${CCM_SERVER_TYPE}/${CCM_VERSION}/${DRIVER_LIBRARY}-integration-tests-driver-logs.tgz *
            )
          fi

          exit 1
        fi
      fi
    '''
  }
}

def buildPackagesAndInstallDriver() {
  sh label: 'Build packages and install driver', script: '''#!/bin/bash -le
    . ${DRIVER_BUILD_SCRIPT}

    install_driver
    if [ -d packaging/packages ]; then
      mkdir -p ${OS_DISTRO}/${OS_DISTRO_RELEASE}
      cp packaging/packages/${DRIVER_LIBRARY}-cpp-driver* ${OS_DISTRO}/${OS_DISTRO_RELEASE}
    fi
  '''
}

def testDriverInstallation() {
  sh label: 'Test driver installation', script: '''#!/bin/bash -le
    . ${DRIVER_BUILD_SCRIPT}

    test_installed_driver "${DRIVER_LIBRARY}"
  '''
}

def deployDriver() {
  withCredentials([usernameColonPassword(credentialsId: 'drivers-artifactory-upload-username-encrypted-password', variable: 'ARTIFACTORY_CREDENTIALS'),
                   string(credentialsId: 'artifactory-base-url', variable: 'ARTIFACTORY_BASE_URL')]) {
    sh label: 'Deploy driver to Artifactory', script: '''#!/bin/bash -le
      . ${DRIVER_BUILD_SCRIPT}

      DRIVER_FOLDER=cpp-driver
      if [ "${DRIVER_TYPE}" = "DSE" ]; then
        DRIVER_FOLDER=cpp-dse-driver
      fi
      DRIVER_BASE="cpp-php-drivers/${DRIVER_FOLDER}/builds"
      curl -u "${ARTIFACTORY_CREDENTIALS}" -T "{$(echo packaging/packages/${DRIVER_LIBRARY}-cpp-driver* | tr ' ' ',')}" "${ARTIFACTORY_BASE_URL}/${DRIVER_BASE}/${DRIVER_VERSION}/${GIT_SHA}/${OS_DISTRO}/${OS_DISTRO_RELEASE}/${DRIVER_LIBRARY}/v${DRIVER_VERSION}/"
      curl -u "${ARTIFACTORY_CREDENTIALS}" -T "{$(echo packaging/packages/libuv* | tr ' ' ',')}" "${ARTIFACTORY_BASE_URL}/${DRIVER_BASE}/${DRIVER_VERSION}/${GIT_SHA}/${OS_DISTRO}/${OS_DISTRO_RELEASE}/dependencies/libuv/v${LIBUV_VERSION}/"
    '''
  }
}

def determineBuildType() {
  if (params.ADHOC_BUILD_TYPE == 'BUILD-AND-EXECUTE-TESTS') {
    return 'adhoc'
  } else if (params.ADHOC_BUILD_TYPE == 'BUILD-DOCUMENTS') {
    return 'documents'
  }

  def buildType = 'commit'
  if (env.IS_BUILDING_RELEASE == 'true') {
    buildType = 'release'
  } else if (params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION') {
    buildType = "${params.CI_SCHEDULE.toLowerCase()}"
  }
  return buildType
}

def notifySlack(status = 'started') {
  // Notify Slack channel for every build except adhoc executions
  if (params.ADHOC_BUILD_TYPE != 'BUILD-AND-EXECUTE-TESTS') {
    // Set the global pipeline scoped environment (this is above each matrix)
    env.BUILD_STATED_SLACK_NOTIFIED = 'true'

    def buildType = determineBuildType()
    if (buildType == 'release') {
      buildType += "v${env.DRIVER_VERSION}"
    }
    buildType = buildType.capitalize()

    def color = 'good' // Green
    if (status.equalsIgnoreCase('aborted')) {
      color = '808080' // Grey
    } else if (status.equalsIgnoreCase('unstable')) {
      color = 'warning' // Orange
    } else if (status.equalsIgnoreCase('failed')) {
      color = 'danger' // Red
    }

    def message = """Build ${status} for ${env.DRIVER_DISPLAY_NAME} [${buildType}]
<${env.GITHUB_BRANCH_URL}|${env.BRANCH_NAME}> - <${env.RUN_DISPLAY_URL}|#${env.BUILD_NUMBER}> - <${env.GITHUB_COMMIT_URL}|${env.GIT_SHA}>"""
    if (params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION') {
      message += " - ${env.OS_VERSION}"
    }
    if (!status.equalsIgnoreCase('Started')) {
      message += """
${status} after ${currentBuild.durationString - ' and counting'}"""
    }

    slackSend color: "${color}",
              channel: "#cpp-driver-dev-bots",
              message: "${message}"
  }
}

def submitCIMetrics(buildType) {
  long durationMs = currentBuild.duration
  long durationSec = durationMs / 1000
  long nowSec = (currentBuild.startTimeInMillis + durationMs) / 1000
  def branchNameNoPeriods = env.BRANCH_NAME.replaceAll('\\.', '_')
  def durationMetric = "okr.ci.cpp.${env.DRIVER_METRIC_TYPE}.${buildType}.${branchNameNoPeriods} ${durationSec} ${nowSec}"

  timeout(time: 1, unit: 'MINUTES') {
    withCredentials([string(credentialsId: 'lab-grafana-address', variable: 'LAB_GRAFANA_ADDRESS'),
                     string(credentialsId: 'lab-grafana-port', variable: 'LAB_GRAFANA_PORT')]) {
      withEnv(["DURATION_METRIC=${durationMetric}"]) {
        sh label: 'Send runtime metrics to labgrafana', script: '''#!/bin/bash -lex
          echo "${DURATION_METRIC}" | nc -q 5 ${LAB_GRAFANA_ADDRESS} ${LAB_GRAFANA_PORT}
        '''
      }
    }
  }
}

def describePerCommitStage() {
  script {
    currentBuild.displayName = "Per-Commit build of ${env.BRANCH_NAME}"
    currentBuild.description = "Per-Commit build, export validation, unit testing, package building, and testing of installed library for branch ${env.BRANCH_NAME}"
  }
}

def describeReleaseAndDeployStage() {
  script {
    currentBuild.displayName = "Build release v${env.DRIVER_VERSION} [${env.GIT_SHA}]"
    currentBuild.description = "Build the DataStax C/C++ driver release v${env.DRIVER_VERSION} [${env.GIT_SHA}] and deploy to Artifactory"
  }
}

def describeScheduledAndAdhocTestingStage() {
  script {
    if (params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION') {
      currentBuild.displayName = "${params.CI_SCHEDULE.toLowerCase().capitalize()} schedule on ${env.OS_VERSION}"
    } else {
      currentBuild.displayName = "${params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION} on ${env.OS_VERSION}"
    }

    def serverVersions = env.SERVER_VERSIONS.split(' ')
    def serverDisplayNames = ''
    serverVersions.each {
      def serverType = it.split('-')[0]
      def serverDisplayName = 'Apache Cassandra&reg;'
      if (serverType == 'ALL') {
        serverDisplayName = "all Apache Cassandra&reg; and DataStax Enterprise server versions"
      } else {
        def serverVersion = it
        try {
          serverVersion = it.split('-')[1]
        } catch (e) {
          ;; // no-op
        }
        if (serverType == 'ddac') {
          serverDisplayName = "DataStax Distribution of ${serverDisplayName}"
        } else if (serverType == 'dse') {
          serverDisplayName = 'DataStax Enterprise'
        }
        serverDisplayNames += "${serverDisplayName} v${serverVersion}.x"
        if (it != serverVersions[-1]) {
          serverDisplayNames += ', '
          if (it != serverVersions[-2]) {
            serverDisplayNames += 'and '
          }
        }
      }
    }
    currentBuild.description = "Testing ${serverDisplayNames} on ${env.OS_VERSION}"
  }
}

def describeScheduledAndAdhocBuildDocuments() {
  script {
    currentBuild.displayName = "Build documents [${env.GIT_SHA}]"
    currentBuild.description = "Build the DataStax C/C++ driver documents [${env.GIT_SHA}]"
  }
}

pipeline {
  agent none

  // Global pipeline timeout
  options {
    timeout(time: 10, unit: 'HOURS')
    buildDiscarder(logRotator(artifactNumToKeepStr: '10', // Keep only the last 10 artifacts
                              numToKeepStr: '50'))        // Keep only the last 50 build records
  }

  parameters {
    choice(
      name: 'ADHOC_BUILD_TYPE',
      choices: ['BUILD', 'BUILD-AND-EXECUTE-TESTS', 'BUILD-RELEASE-AND-DEPLOY', 'BUILD-DOCUMENTS'],
      description: '''Perform a adhoc build operation
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>BUILD</strong></td>
                          <td>Performs a <b>Per-Commit</b> build</td>
                        </tr>
                        <tr>
                          <td><strong>BUILD-AND-EXECUTE-TESTS</strong></td>
                          <td>
                            Performs a build and executes the integration and unit tests<br/>
                            <ul>
                              <li>Use <b>ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION</b> to change Apache Cassandra&reg; and DataStax Enterprise selection</li>
                              <li>Use <b>OS_VERSION</b> to change operating system selection</li>
                              <li>USE <b>INTEGRATION_TESTS_FILTER</b> to limit integration tests</li>
                            </ul>
                          </td>
                        </tr>
                        <tr>
                          <td><strong>BUILD-RELEASE-AND-DEPLOY</strong></td>
                          <td>Performs a release build and deploys to Artifactory</td>
                        </tr>
                        <tr>
                          <td><strong>BUILD-DOCUMENTS</strong></td>
                          <td>Performs a document build using documentor</td>
                        </tr>
                      </table>
                      <br/>''')
    choice(
      name: 'ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION',
      choices: ['2.1',      // Legacy Apache Cassandra®
                '2.2',      // Legacy Apache Cassandra®
                '3.0',      // Previous Apache Cassandra®
                '3.11',     // Current Apache Cassandra®
                '4.0',      // Development Apache Cassandra®
                'ddac-5.1', // Current DataStax Distribution of Apache Cassandra®
                'dse-4.8',  // Previous EOSL DataStax Enterprise
                'dse-5.0',  // Long Term Support DataStax Enterprise
                'dse-5.1',  // Legacy DataStax Enterprise
                'dse-6.0',  // Previous DataStax Enterprise
                'dse-6.7',  // Current DataStax Enterprise
                'dse-6.8',  // Development DataStax Enterprise
                'ALL'],
      description: '''Apache Cassandra&reg; and DataStax Enterprise server version to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> builds
                      <table style="width:100%">
                        <col width="15%">
                        <col width="85%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>2.1</strong></td>
                          <td>Apache Cassandra&reg; v2.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>2.2</strong></td>
                          <td>Apache Cassandra&reg; v2.2.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.0</strong></td>
                          <td>Apache Cassandra&reg; v3.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.11</strong></td>
                          <td>Apache Cassandra&reg; v3.11.x</td>
                        </tr>
                        <tr>
                          <td><strong>4.0</strong></td>
                          <td>Apache Cassandra&reg; v4.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>ddac-5.1</strong></td>
                          <td>DataStax Distribution of Apache Cassandra&reg; v5.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-4.8</strong></td>
                          <td>DataStax Enterprise v4.8.x (<b>END OF SERVICE LIFE</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>dse-5.0</strong></td>
                          <td>DataStax Enterprise v5.0.x (<b>Long Term Support</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>dse-5.1</strong></td>
                          <td>DataStax Enterprise v5.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.0</strong></td>
                          <td>DataStax Enterprise v6.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.7</strong></td>
                          <td>DataStax Enterprise v6.7.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.8</strong></td>
                          <td>DataStax Enterprise v6.8.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
                        </tr>
                      </table>''')
    choice(
      name: 'OS_VERSION',
      choices: ['centos/6-64/cpp',
                'centos/7-64/cpp',
                'centos/8-64/cpp',
                'ubuntu/trusty64/cpp',
                'ubuntu/xenial64/cpp',
                'ubuntu/bionic64/cpp'],
      description: '''Operating system to use for scheduled or adhoc builds
                      <table style="width:100%">
                        <col width="20%">
                        <col width="80%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>centos/6-64/cpp</strong></td>
                          <td>CentOS 6 x86_64</td>
                        </tr>
                        <tr>
                          <td><strong>centos/7-64/cpp</strong></td>
                          <td>CentOS 7 x86_64</td>
                        </tr>
                        <tr>
                          <td><strong>centos/8-64/cpp</strong></td>
                          <td>CentOS 8 x86_64</td>
                        </tr>
                        <tr>
                          <td><strong>ubuntu/trusty64/cpp</strong></td>
                          <td>Ubuntu 14.04 LTS x86_64</td>
                        </tr>
                        <tr>
                          <td><strong>ubuntu/xenial64/cpp</strong></td>
                          <td>Ubuntu 16.04 LTS x86_64</td>
                        </tr>
                        <tr>
                          <td><strong>ubuntu/bionic64/cpp</strong></td>
                          <td>Ubuntu 18.04 LTS x86_64</td>
                        </tr>
                      </table>''')
    string(
      name: 'INTEGRATION_TESTS_FILTER',
      defaultValue: '',
      description: '''<p><b>POSITIVE_PATTERNS[-NEGATIVE_PATTERNS]</b></p>
                      Run only the tests whose name matches one of the positive patterns but none of the negative patterns. <b>\'?\'</b> matches any single character; <b>\'*\'</b> matches any substring; <b>\':\'</b> separates two patterns.''')
    string(
      name: 'LIBUV_VERSION',
      defaultValue: '1.34.0',
      description: '''<p>libuv version to build and use</p>
                      <b>Note:</b> Rarely does this need to change.''')
    choice(
      name: 'CI_SCHEDULE',
      choices: ['DO-NOT-CHANGE-THIS-SELECTION', 'WEEKNIGHTS', 'WEEKENDS', 'MONTHLY'],
      description: 'CI testing schedule to execute periodically scheduled builds and tests of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
    string(
      name: 'CI_SCHEDULE_SERVER_VERSIONS',
      defaultValue: 'DO-NOT-CHANGE-THIS-SELECTION',
      description: 'CI testing server version(s) to utilize for scheduled test runs of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
  }

  triggers {
    parameterizedCron("""
      # Every weeknight (Monday - Friday) around 12:00 AM
      H 0 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;CI_SCHEDULE_SERVER_VERSIONS=ALL;OS_VERSION=centos/8-64/cpp;INTEGRATION_TESTS_FILTER=CassandraTypes/*:DseTypes/*
      H 0 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;CI_SCHEDULE_SERVER_VERSIONS=ALL;OS_VERSION=ubuntu/bionic64/cpp;INTEGRATION_TESTS_FILTER=CassandraTypes/*:DseTypes/*
      # Every weekend (Saturday) around 12:00 AM
      H 0 * * 6 %CI_SCHEDULE=WEEKENDS;CI_SCHEDULE_SERVER_VERSIONS=3.11 dse-5.1 dse-6.0 dse-6.7;OS_VERSION=centos/8-64/cpp
      H 0 * * 6 %CI_SCHEDULE=WEEKENDS;CI_SCHEDULE_SERVER_VERSIONS=3.11 dse-5.1 dse-6.0 dse-6.7;OS_VERSION=ubuntu/bionic64/cpp
      H 0 * * 6 %CI_SCHEDULE=WEEKENDS;ADHOC_BUILD_TYPE=BUILD-DOCUMENTS
      # Every month on the first around 12:00 AM
      # H 0 1 * * %CI_SCHEDULE=MONTHLY;CI_SCHEDULE_SERVER_VERSIONS=4.0 ddac-5.1 dse-6.8;OS_VERSION=centos/8-64/cpp
      # H 0 1 * * %CI_SCHEDULE=MONTHLY;CI_SCHEDULE_SERVER_VERSIONS=4.0 ddac-5.1 dse-6.8;OS_VERSION=ubuntu/bionic64/cpp
    """)
  }

  environment {
    LIBUV_VERSION = "${params.LIBUV_VERSION}"
    CCM_ENVIRONMENT_SHELL = '/usr/local/bin/ccm_environment.sh'
  }

  stages {
    stage('Per-Commit') {
      options {
        timeout(time: 1, unit: 'HOURS')
      }
      when {
        beforeAgent true
        allOf {
          expression { params.ADHOC_BUILD_TYPE == 'BUILD' }
          expression { params.CI_SCHEDULE == 'DO-NOT-CHANGE-THIS-SELECTION' }
          expression { params.CI_SCHEDULE_SERVER_VERSIONS == 'DO-NOT-CHANGE-THIS-SELECTION' }
        }
      }

      matrix {
        axes {
          axis {
            name 'OS_VERSION'
            values 'centos/6-64/cpp',
                   'centos/7-64/cpp',
                   'centos/8-64/cpp',
                   'ubuntu/trusty64/cpp',
                   'ubuntu/xenial64/cpp',
                   'ubuntu/bionic64/cpp'
          }
        }

        agent {
          label "${env.OS_VERSION}"
        }

        stages {
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
              script {
                if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
                  notifySlack()
                }
              }
            }
          }
          stage('Describe-Build') {
            steps {
              describePerCommitStage()
            }
          }
          stage('Install-Dependencies') {
            steps {
              installDependencies()
            }
            post {
              success {
                // Allow empty results for 'osx/high-sierra' which doesn't produce packages
                archiveArtifacts artifacts: "${env.OS_DISTRO}/**/libuv*", allowEmptyArchive: true
              }
            }
          }
          stage('Build-Driver') {
            steps {
              buildDriver()
            }
            post {
              success {
                archiveArtifacts artifacts: "${env.OS_DISTRO}/**/cassandra-*-tests"
                archiveArtifacts artifacts: "${env.OS_DISTRO}/**/dse-*-tests", allowEmptyArchive: true
              }
              failure {
                archiveArtifacts artifacts: "${env.OS_DISTRO}/**/CMakeOutput.log"
                archiveArtifacts artifacts: "${env.OS_DISTRO}/**/CMakeError.log"
              }
            }
          }
          stage('Validate-Driver-Exports') {
            steps {
              validateDriverExports()
            }
          }
          stage('Execute-Unit-Tests') {
            steps {
              configureTestingEnvironment()
              executeUnitTests()
            }
            post {
              always {
                // Allow empty results if segfault occurs
                junit testResults: '*unit-tests-*-results.xml', allowEmptyResults: true
              }
            }
          }
          stage('Build-Packages-And-Install-Driver') {
            when {
              expression { env.OS_VERSION != 'osx/high-sierra' }
            }
            steps {
              buildPackagesAndInstallDriver()
            }
            post {
              success {
                archiveArtifacts artifacts: "${env.OS_DISTRO}/**/*-cpp-driver*"
              }
            }
          }
          stage('Test-Driver-Installation') {
            when {
              expression { env.OS_VERSION != 'osx/high-sierra' }
            }
            steps {
              testDriverInstallation()
            }
          }
        }
        post {
          cleanup {
            cleanWs()
          }
        }
      }
      post {
        always {
          node('master') {
            submitCIMetrics('commit')
          }
        }
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('FAILED')
        }
      }
    }

    stage('Build-Release-And-Deploy') {
      options {
        timeout(time: 1, unit: 'HOURS')
      }
      when {
        beforeAgent true
        anyOf {
          buildingTag()
          expression { params.ADHOC_BUILD_TYPE == 'BUILD-RELEASE-AND-DEPLOY' }
        }
      }

      matrix {
        axes {
          axis {
            name 'OS_VERSION'
            values 'centos/6-64/cpp',
                   'centos/7-64/cpp',
                   'centos/8-64/cpp',
                   'ubuntu/trusty64/cpp',
                   'ubuntu/xenial64/cpp',
                   'ubuntu/bionic64/cpp'
          }
        }

        environment {
          IS_BUILDING_RELEASE = 'true'
        }
        agent {
          label "${env.OS_VERSION}"
        }

        stages {
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
              script {
                if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
                  notifySlack()
                }
              }
            }
          }
          stage('Describe-Release-And-Deploy') {
            steps {
              describeReleaseAndDeployStage()
            }
          }
          stage('Install-Dependencies') {
            steps {
              installDependencies()
            }
          }
          stage('Build-Packages-And-Install-Driver') {
            steps {
              buildPackagesAndInstallDriver()
            }
          }
          stage('Deploy-Driver') {
            steps {
              deployDriver()
            }
          }
        }
        post {
          cleanup {
            cleanWs()
          }
        }
      }
      post {
        always {
          node('master') {
            submitCIMetrics('release')
          }
        }
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('FAILED')
        }
      }
    }

    stage('Scheduled-And-Adhoc-Testing') {
      when {
        beforeAgent true
        allOf {
          not { buildingTag() }
          anyOf {
            expression { params.ADHOC_BUILD_TYPE == 'BUILD-AND-EXECUTE-TESTS' }
            allOf {
              expression { params.ADHOC_BUILD_TYPE == 'BUILD' }
              expression { params.CI_SCHEDULE != 'DO-NOT-CHANGE-THIS-SELECTION' }
              expression { params.CI_SCHEDULE_SERVER_VERSIONS != 'DO-NOT-CHANGE-THIS-SELECTION' }
            }
          }
        }
      }

      environment {
        CI_INTEGRATION_ENABLED = 'true'
        TEST_FILTER = "${params.INTEGRATION_TESTS_FILTER}"
        OS_VERSION = "${params.OS_VERSION}"
        SERVER_VERSIONS = "${params.CI_SCHEDULE_SERVER_VERSIONS == 'DO-NOT-CHANGE-THIS-SELECTION' ? params.ADHOC_BUILD_AND_EXECUTE_TESTS_SERVER_VERSION : params.CI_SCHEDULE_SERVER_VERSIONS}"
      }

      matrix {
        axes {
          axis {
            name 'SERVER_VERSION'
            values '2.1',      // Legacy Apache Cassandra®
                   '2.2',      // Legacy Apache Cassandra®
                   '3.0',      // Previous Apache Cassandra®
                   '3.11',     // Current Apache Cassandra®
                   '4.0',      // Development Apache Cassandra®
                   'ddac-5.1', // Current DataStax Distribution of Apache Cassandra®
                   'dse-4.8',  // Previous EOSL DataStax Enterprise
                   'dse-5.0',  // Long Term Support DataStax Enterprise
                   'dse-5.1',  // Legacy DataStax Enterprise
                   'dse-6.0',  // Previous DataStax Enterprise
                   'dse-6.7',  // Current DataStax Enterprise
                   'dse-6.8'   // Development DataStax Enterprise
          }
        }
        when {
          beforeAgent true
          allOf {
            expression { return env.SERVER_VERSIONS.split(' ').any { it =~ /(ALL|${env.SERVER_VERSION})/ } }
          }
        }

        agent {
          label "${params.OS_VERSION}"
        }

        stages {
          stage('Initialize-Environment') {
            steps {
              initializeEnvironment()
              script {
                if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
                  notifySlack()
                }
              }
            }
          }
          stage('Describe-Build') {
            steps {
              describeScheduledAndAdhocTestingStage()
            }
          }
          stage('Install-Dependencies') {
            steps {
              installDependencies()
            }
          }
          stage('Build-Driver') {
            steps {
              buildDriver()
            }
            post {
              failure {
                archiveArtifacts artifacts: "${env.OS_DISTRO}/**/CMakeOutput.log"
                archiveArtifacts artifacts: "${env.OS_DISTRO}/**/CMakeError.log"
              }
            }
          }
          stage('Execute-Integration-Tests') {
            steps {
              configureTestingEnvironment()
              executeIntegrationTests()
            }
            post {
              always {
                // Allow empty results for when segfaults occur
                junit testResults: '*integration-tests-*-results.xml', allowEmptyResults: true
              }
              failure {
                archiveArtifacts artifacts: "${env.OS_DISTRO}/**/*-integration-tests-driver-logs.tgz"
              }
              cleanup {
                cleanWs()
              }
            }
          }
        }
      }
      post {
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('FAILED')
        }
      }
    }

    stage('Scheduled-And-Adhoc-Build-Documents') {
      when {
        beforeAgent true
        allOf {
          not { buildingTag() }
          anyOf {
            allOf {
              // User initiated
              expression { params.ADHOC_BUILD_TYPE == 'BUILD-DOCUMENTS' }
              expression { params.CI_SCHEDULE == 'DO-NOT-CHANGE-THIS-SELECTION' }
              expression { params.CI_SCHEDULE_SERVER_VERSIONS == 'DO-NOT-CHANGE-THIS-SELECTION' }
            }
            allOf {
              // Schedule initiated
              branch 'master'
              expression { params.ADHOC_BUILD_TYPE == 'BUILD-DOCUMENTS' }
              expression { params.CI_SCHEDULE == 'WEEKENDS' }
              expression { params.CI_SCHEDULE_SERVER_VERSIONS == 'DO-NOT-CHANGE-THIS-SELECTION' }
            }
          }
        }
      }

      environment {
        OS_VERSION = 'ubuntu/bionic64/cpp'
      }
      agent {
        label 'ubuntu/bionic64/cpp'
      }

      stages {
        stage('Initialize-Environment') {
          steps {
            initializeEnvironment()
            script {
              notifySlack()
            }
          }
        }
        stage('Describe-Build') {
          steps {
            describeScheduledAndAdhocBuildDocuments()
          }
        }
        stage('Build-Documents') {
          steps {
            buildDocuments()
          }
          post {
            success {
              archiveArtifacts artifacts: '*-documents.tgz'
            }
          }
        }
      }
      post {
        aborted {
          notifySlack('aborted')
        }
        success {
          notifySlack('completed')
        }
        unstable {
          notifySlack('unstable')
        }
        failure {
          notifySlack('FAILED')
        }
      }
    }
  }
}
