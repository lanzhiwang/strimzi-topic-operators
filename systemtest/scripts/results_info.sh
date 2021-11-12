#!/usr/bin/env bash

RESULTS_PATH=${1}
TEST_CASE=${2}
TEST_PROFILE=${3}
BUILD_ID=${4:-0}
OCP_VERSION=${5:-3}

JSON_FILE_RESULTS=results.json

function get_test_count () {
    _TEST_TYPE=${1}
    _VALUES=$(find "${RESULTS_PATH}" -name "TEST*.xml" -type f -print0 | xargs -0 sed -n "s#.*${_TEST_TYPE}=\"\([0-9]*\)\".*#\1#p")
    _TEST_COUNTS_ARR=$(echo "${_VALUES}" | tr " " "\n")
    _TEST_COUNT=0

    for item in ${_TEST_COUNTS_ARR}
    do
        _TEST_COUNT=$((_TEST_COUNT + item))
    done

    echo ${_TEST_COUNT}
}

TEST_COUNT=$(get_test_count "tests")
TEST_ERRORS_COUNT=$(get_test_count "errors")
TEST_SKIPPED_COUNT=$(get_test_count "skipped")
TEST_FAILURES_COUNT=$(get_test_count "failures")

if [[ "${OCP_VERSION}" == "4" ]]; then
  BUILD_ENV="crc"
else
  BUILD_ENV="oc cluster up"
fi

TEST_ALL_FAILED_COUNT=$((TEST_ERRORS_COUNT + TEST_FAILURES_COUNT))

SUMMARY="**TEST_PROFILE**: ${TEST_PROFILE}\n**TEST_CASE:** ${TEST_CASE}\n**TOTAL:** ${TEST_COUNT}\n**PASS:** $((TEST_COUNT - TEST_ALL_FAILED_COUNT - TEST_SKIPPED_COUNT))\n**FAIL:** ${TEST_ALL_FAILED_COUNT}\n**SKIP:** ${TEST_SKIPPED_COUNT}\n**BUILD_NUMBER:** ${BUILD_ID}\n**BUILD_ENV:** ${BUILD_ENV}\n"


FAILED_TESTS=$(find "${RESULTS_PATH}" -name 'TEST*.xml' -type f -print0 | xargs -0 sed -n "s#\(<testcase.*time=\"[0-9]*,\{0,1\}[0-9]\{1,3\}\..*[^\/]>\)#\1#p" | awk -F '"' '{print "\\n- " $2 " in "  $4}')
echo ${FAILED_TESTS}
echo "Creating body ..."

TMP_FAILED_TESTS=$(find "${RESULTS_PATH}" -name 'TEST*.xml' -type f -print0 | xargs -0 sed -n "s#\(<testcase.*time=\"[0-9]*,\{0,1\}[0-9]\{1,3\}\..*[^\/]>\)#\1#p" | awk -F '"' '{print "" $4 "#" $2}')
COMMAND="@strimzi-ci run tests profile=${TEST_PROFILE} testcase="

for line in ${TMP_FAILED_TESTS}
do
  # Compare test method name and test class name
  IFS='#' read -ra TEST_NAME <<< "${line}"
  if [[ ${TEST_NAME[0]} == ${TEST_NAME[1]} ]]
  then
    COMMAND="${COMMAND}${TEST_NAME[1]},"
  else
    COMMAND="${COMMAND}${line},"
  fi
done

echo "Re-run command:"
echo ${COMMAND::-1}


if [ -n "${FAILED_TESTS}" ]
then
  FAILED_TEST_BODY="### :heavy_exclamation_mark: Test Failures :heavy_exclamation_mark:${FAILED_TESTS}"
fi

if [[ "${BUILD_PROJECT_STATUS}" == "false" ]]
then
  BODY="{\"body\":\":heavy_exclamation_mark: **Build project failed** :heavy_exclamation_mark:\"}"
elif [[ "${BUILD_DOCKER_IMAGE_STATUS}" == "false" ]]
then
  BODY="{\"body\":\":heavy_exclamation_mark: **Image build failed (unit tests or build)** :heavy_exclamation_mark:\"}"
elif [ "${TEST_COUNT}" == 0 ]
then
  BODY="{\"body\":\":heavy_exclamation_mark: **Systemtests Failed (no tests results are present)** :heavy_exclamation_mark:\"}"
else
  if [ "${TEST_ALL_FAILED_COUNT}" == 0 ]
  then
    BODY="{\"body\":\"### :heavy_check_mark: Test Summary :heavy_check_mark:\n${SUMMARY}${FAILED_TEST_BODY}\"}"
  else
    BODY="{\"body\":\"### :x: Test Summary :x:\n${SUMMARY}${FAILED_TEST_BODY}\n\n**Re-run command**:\n${COMMAND::-1}\"}"
  fi
fi

echo "${BODY}" > ${JSON_FILE_RESULTS}

# Cat created file
cat ${JSON_FILE_RESULTS}
