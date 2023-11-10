/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.admin.om.ContainerCleanupSubCommand;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test class for `ozone admin om containerCleanup` CLI that deletes keys
 * and containers.
 */
public class TestContainerCleanupSubCommand {
  private static final String KEY_TABLE = "keyTable";
  private static final String FILE_TABLE = "fileTable";
  private OzoneConfiguration conf;
  private DBStore dbStore;
  @TempDir
  private File tempDir;
  private StringWriter stdout, stderr;
  private PrintWriter pstdout, pstderr;
  private CommandLine cmd;

  @BeforeEach
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    stdout = new StringWriter();
    pstdout = new PrintWriter(stdout);
    stderr = new StringWriter();
    pstderr = new PrintWriter(stderr);

    cmd = new CommandLine(new ContainerCleanupSubCommand())
        .setOut(pstdout)
        .setErr(pstderr);

    dbStore = DBStoreBuilder.newBuilder(conf).setName("om.db")
        .setPath(tempDir.toPath()).addTable(KEY_TABLE).addTable(FILE_TABLE)
        .build();
  }

  @AfterEach
  public void shutdown() throws IOException {
    if (dbStore != null) {
      dbStore.close();
    }
    pstderr.close();
    stderr.close();
    pstdout.close();
    stdout.close();
  }

  @Test
  void testWhenKeysAndContainersDeleted() throws IOException {

    // create keys for tables
    createTableKey("key1", FILE_TABLE, 1L);
    createTableKey("key2", KEY_TABLE, 2L);
    createTableKey("key3", KEY_TABLE, 3L);

    String[] cmdArgs =
        {"--db", dbStore.getDbLocation().getAbsolutePath(),
            "-ids", "1,2,3", "-om-id", "om", "-om-host", "om"};

    int exitCode = cmd.execute(cmdArgs);
    Assertions.assertEquals(0, exitCode);

    // TODO fix expected output
    String expectedOutput = "";
    Assertions.assertEquals(expectedOutput, stdout.toString());

    Assertions.assertTrue(stderr.toString().isEmpty());
  }

  private void createTableKey(String keyName, String tableName,
                              Long containerId)
      throws IOException {
    Table<byte[], byte[]> table = dbStore.getTable(tableName);

    // generate table key
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String key = null;
    if (tableName.equals(FILE_TABLE)) {
      // /volumeId/bucketId/parentId(bucketId)/keyName
      key = "/-123/-456/-456/" + keyName;
    } else {
      // /volumeName/bucketName/keyName
      key = "/" + volumeName + "/" + bucketName + "/" + keyName;
    }

    // generate table value
    OmKeyInfo value =
        getOmKeyInfo(volumeName, bucketName, keyName, containerId);

    table.put(key.getBytes(UTF_8),
        value.getProtobuf(ClientVersion.CURRENT_VERSION).toByteArray());
  }

  private static OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                        String keyName, long containerId) {
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName,
        keyName, HddsProtos.ReplicationType.STAND_ALONE,
        HddsProtos.ReplicationFactor.ONE, 1, 1, 1, false, new ArrayList<>(
            Collections.singletonList(
                new OmKeyLocationInfo.Builder().setBlockID(
                    new BlockID(containerId, 1)).build())));
  }

}
