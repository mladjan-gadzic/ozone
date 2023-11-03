/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.debug;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ContainerKeyScanner}.
 */
@ExtendWith(MockitoExtension.class)
public class TestContainerKeyScanner {

  private ContainerKeyScanner containerKeyScanner;
  @Mock
  private ContainerKeyInfoWrapper containerKeyInfoWrapper;

  @BeforeEach
  void setup() {
    containerKeyScanner = new ContainerKeyScanner();
    containerKeyScanner.setContainerIds(
        Stream.of(1L, 2L, 3L).collect(Collectors.toSet()));
  }

  @Test
  void testOutputWhenContainerKeyInfosEmpty() {
    when(containerKeyInfoWrapper.getContainerKeyInfos())
        .thenReturn(new ArrayList<>());
    long processedKeys = new Random().nextLong();
    when(containerKeyInfoWrapper.getKeysProcessed()).thenReturn(processedKeys);

    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));

    containerKeyScanner.printOutput(containerKeyInfoWrapper);

    String expectedOutput = "No keys were found for container IDs: " +
        containerKeyScanner.getContainerIds() + "\n" +
        "Keys processed: " + processedKeys + "\n";
    assertEquals(expectedOutput, outContent.toString());
  }

  @Test
  void testOutputWhenContainerKeyInfosNotEmptyAndKeyMatchesContainerId() {
    List<ContainerKeyInfo> containerKeyInfos = Stream.of(
        new ContainerKeyInfo(1L, "vol1", "bucket1", "key1"),
        new ContainerKeyInfo(2L, "vol2", "bucket2", "key2"),
        new ContainerKeyInfo(3L, "vol3", "bucket3", "key3")
    ).collect(Collectors.toList());
    when(containerKeyInfoWrapper.getContainerKeyInfos())
        .thenReturn(containerKeyInfos);
    long processedKeys = containerKeyInfos.size();
    when(containerKeyInfoWrapper.getKeysProcessed()).thenReturn(processedKeys);

    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));

    containerKeyScanner.printOutput(containerKeyInfoWrapper);

    String expectedOutput = "{\n" +
        "  \"keysProcessed\": 3,\n" +
        "  \"containerKeys\": {\n" +
        "    \"1\": [\n" +
        "      {\n" +
        "        \"containerID\": 1,\n" +
        "        \"volumeName\": \"vol1\",\n" +
        "        \"bucketName\": \"bucket1\",\n" +
        "        \"keyName\": \"key1\"\n" +
        "      }\n" +
        "    ],\n" +
        "    \"2\": [\n" +
        "      {\n" +
        "        \"containerID\": 2,\n" +
        "        \"volumeName\": \"vol2\",\n" +
        "        \"bucketName\": \"bucket2\",\n" +
        "        \"keyName\": \"key2\"\n" +
        "      }\n" +
        "    ],\n" +
        "    \"3\": [\n" +
        "      {\n" +
        "        \"containerID\": 3,\n" +
        "        \"volumeName\": \"vol3\",\n" +
        "        \"bucketName\": \"bucket3\",\n" +
        "        \"keyName\": \"key3\"\n" +
        "      }\n" +
        "    ]\n" +
        "  }\n" +
        "}\n";
    assertEquals(expectedOutput, outContent.toString());
  }

  @Test
  void testOutputWhenContainerKeyInfosNotEmptyAndKeysDoNotMatchContainersId() {
    List<ContainerKeyInfo> containerKeyInfos = Stream.of(
        new ContainerKeyInfo(4L, "vol1", "bucket1", "key1"),
        new ContainerKeyInfo(5L, "vol2", "bucket2", "key2"),
        new ContainerKeyInfo(6L, "vol3", "bucket3", "key3")
    ).collect(Collectors.toList());
    when(containerKeyInfoWrapper.getContainerKeyInfos())
        .thenReturn(containerKeyInfos);
    long processedKeys = containerKeyInfos.size();
    when(containerKeyInfoWrapper.getKeysProcessed()).thenReturn(processedKeys);

    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));

    containerKeyScanner.printOutput(containerKeyInfoWrapper);

    String expectedOutput = "{\n" +
        "  \"keysProcessed\": 3,\n" +
        "  \"containerKeys\": {\n" +
        "    \"1\": [],\n" +
        "    \"2\": [],\n" +
        "    \"3\": []\n" +
        "  }\n" +
        "}\n";
    assertEquals(expectedOutput, outContent.toString());
  }

}
