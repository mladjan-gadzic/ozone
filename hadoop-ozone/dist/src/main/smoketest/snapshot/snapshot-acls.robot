# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

*** Settings ***
Documentation       Test for reading Snapshot with acls
Library             OperatingSystem
Resource            ../ozone-lib/shell.robot
Resource            snapshot-setup.robot
Test Timeout        5 minutes

*** Variables ***
${USER1} =              testuser
${USER2} =              testuser2
${VOLUME}
${BUCKET}
${KEY}
${SNAPSHOT_PREFIX}      .snapshot
${SNAPSHOT_ONE}
${SNAPSHOT_TWO}

*** Keywords ***
Kinit ozone user1
    Run Keyword if  '${SECURITY_ENABLED}' == 'true'     Kinit test user     ${USER1}     ${USER1}.keytab

Kinit ozone user2
    Run Keyword if  '${SECURITY_ENABLED}' == 'true'     Kinit test user     ${USER2}     ${USER2}.keytab

Create volume
    ${random} =         Generate Random String  5   [LOWER]
    Set Suite Variable  ${VOLUME}               vol-${random}
    ${result} =         Execute                 ozone sh volume create /${VOLUME}
    Should not contain  ${result}               Failed

Create bucket
    ${random} =         Generate Random String  5   [LOWER]
    Set Suite Variable  ${BUCKET}               buc-${random}
    ${result} =         Execute                 ozone sh bucket create -l FILE_SYSTEM_OPTIMIZED /${VOLUME}/${BUCKET}
    Should not contain  ${result}               Failed

Create key
    ${random} =         Generate Random String  5  [LOWER]
    Set Suite Variable  ${KEY}              key-${random}
    # TODO: remove /opt/hadoop/
    ${result} =         Execute                 ozone sh key put /${VOLUME}/${BUCKET}/${KEY} README.md
    Should not contain  ${result}               Failed
Create snapshot
    ${random} =         Generate Random String  5   [LOWER]
    ${snapshot_name} =  Set Variable            snap-${random}
    ${result} =         Execute                 ozone sh snapshot create /${VOLUME}/${BUCKET} ${snapshot_name}
    Should not contain  ${result}               Failed
    [return]            ${snapshot_name}

Add ACL
    [arguments]         ${object}   ${user}     ${objectName}
    ${result} =         Execute     ozone sh ${object} addacl -a user:${user}/scm@EXAMPLE.COM:a ${objectName}
    Should not contain  ${result}   Failed

Add ACLs
    Add ACL     volume      ${USER1}    ${VOLUME}
    Add ACL     bucket      ${USER1}    ${VOLUME}/${BUCKET}
    Add ACL     key         ${USER1}    ${VOLUME}/${BUCKET}/${KEY}

Get key
    [arguments]     ${snapshotName}     ${keyDest}
    ${result} =     Execute             ozone sh key get ${VOLUME}/${BUCKET}/${SNAPSHOT_PREFIX}/${snapshotName}/${KEY} ${keyDest}
    [return]        ${result}

*** Test Cases ***
Test initialize prerequisites and create a Snapshot as user1
    Execute             kdestroy
    Kinit ozone user1
    Create volume
    Create bucket
    Create key
    ${snapshot_name} =  Create snapshot
    Set Suite Variable  ${SNAPSHOT_ONE}     ${snapshot_name}

Test add ACLs and create a Snapshot
    Add ACLs
    Set Suite Variable  ${SNAPSHOT_TWO}     Create snapshot

Test read Snapshots as user2
    Execute             kdestroy
    Kinit ozone user2
    ${keyDest} =        Generate Random String  5   [LOWER]
    ${result} =         Get key                 ${SNAPSHOT_ONE}     ${keyDest}
    Should contain      ${result}               PERMISSION_DENIED