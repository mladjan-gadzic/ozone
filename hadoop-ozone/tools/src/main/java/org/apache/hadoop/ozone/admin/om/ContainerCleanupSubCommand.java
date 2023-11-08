/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.admin.om;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.debug.ContainerKeyInfo;
import org.apache.hadoop.ozone.debug.ContainerKeyInfoWrapper;
import org.apache.hadoop.ozone.debug.ContainerKeyScanner;
import org.apache.hadoop.ozone.debug.DBDefinitionFactory;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * Handler of ozone admin om containerCleanup command.
 */
@CommandLine.Command(
    name = "containerCleanup",
    description = "Command used for cleaning up " +
        "missing containers.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class ContainerCleanupSubCommand implements Callable<Void> {

  @CommandLine.Mixin
  private ScmOption scmOption;

  @CommandLine.ParentCommand
  private OMAdmin parent;

  @CommandLine.Option(names = {"-ids", "--container-ids"},
      split = ",",
      paramLabel = "containerIDs",
      required = true,
      description = "Set of container IDs to be used for getting all " +
          "their keys. Example-usage: 1,11,2.(Separated by ',')")
  private Set<Long> containerIds;

  @CommandLine.Option(names = {"--db"},
      required = true,
      description = "Database File Path")
  private String dbPath;

  @CommandLine.Option(
      names = {"-om-id", "--om-service-id"},
      description = "Ozone Manager Service ID"
  )
  private String omServiceId;

  @CommandLine.Option(
      names = {"-om-host", "--om-service-host"},
      description = "Ozone Manager Host"
  )
  private String omHost;

  @Override
  public Void call() throws Exception {
    ContainerKeyScanner containerKeyScanner = new ContainerKeyScanner();
    ContainerKeyInfoWrapper containerKeyInfoWrapper =
        containerKeyScanner.scanDBForContainerKeys(dbPath, containerIds);

    ContainerKeyInfo containerKeyInfo =
        containerKeyInfoWrapper.getContainerKeyInfos().get(0);

    long bucketId = Long.parseLong(containerKeyInfo.getBucketId());
    String prefix =
        OM_KEY_PREFIX + containerKeyInfo.getVolumeId() + OM_KEY_PREFIX +
            containerKeyInfo.getBucketId() + OM_KEY_PREFIX;
    Set<Long> dirObjIds = new HashSet<>();
    dirObjIds.add(containerKeyInfo.getParentId());
    dirObjIds.add(
        containerKeyInfoWrapper.getContainerKeyInfos().get(1).getParentId());
    dirObjIds.add(
        containerKeyInfoWrapper.getContainerKeyInfos().get(2).getParentId());
    Map<Long, Path> absolutePathForObjectIDs =
        getAbsolutePathForObjectIDs(bucketId, prefix, Optional.of(dirObjIds));

    /*
    // TODO check how to instantiate a client
    try (
        OzoneManagerProtocol client = parent.createOmClient(omServiceId, omHost,
            false)) {
      OmKeyArgs omKeyArgs = new OmKeyArgs.Builder()
          .setVolumeName(containerKeyInfo.getVolumeName())
          .setBucketName(containerKeyInfo.getBucketName())
          // TODO add key name by parsing abs path and extend scanner by objectId of a key
          .setKeyName()
          .build();
      client.deleteKey(omKeyArgs);
    }
     */

    // TODO fix container deletion
    try (ScmClient client = scmOption.createScmClient(
        parent.getParent().getOzoneConf())) {
      client.deleteContainer(1, true);
    }

    System.out.println("Called container cleanup");
    return null;
  }

  public Map<Long, Path> getAbsolutePathForObjectIDs(
      long bucketId, String prefix, Optional<Set<Long>> dirObjIds)
      throws IOException, RocksDBException {
    // Root of a bucket would always have the
    // key as /volumeId/bucketId/bucketId/
    if (!dirObjIds.isPresent() || dirObjIds.get().isEmpty()) {
      return Collections.emptyMap();
    }
    Set<Long> objIds = Sets.newHashSet(dirObjIds.get());
    Map<Long, Path> objectIdPathMap = new HashMap<>();
    Queue<Pair<Long, Path>> objectIdPathVals = new LinkedList<>();
    Pair<Long, Path> root = Pair.of(bucketId, Paths.get(OZONE_URI_DELIMITER));
    objectIdPathVals.add(root);
    addToPathMap(root, objIds, objectIdPathMap);

    while (!objectIdPathVals.isEmpty() && !objIds.isEmpty()) {
      Pair<Long, Path> parentPair = objectIdPathVals.poll();
      // read directoryTable
      List<ColumnFamilyDescriptor> columnFamilyDescriptors =
          RocksDBUtils.getColumnFamilyDescriptors(dbPath);
      final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

      try (ManagedRocksDB db = ManagedRocksDB.openReadOnly(dbPath,
          columnFamilyDescriptors, columnFamilyHandles)) {
        dbPath = removeTrailingSlashIfNeeded(dbPath);
        DBDefinition dbDefinition = DBDefinitionFactory.getDefinition(
            Paths.get(dbPath), new OzoneConfiguration());
        if (dbDefinition == null) {
          throw new IllegalStateException("Incorrect DB Path");
        }

        String tableName = "directoryTable";
        DBColumnFamilyDefinition<?, ?> columnFamilyDefinition =
            dbDefinition.getColumnFamily(tableName);
        if (columnFamilyDefinition == null) {
          throw new IllegalStateException(
              "Table with name" + tableName + " not found");
        }

        ColumnFamilyHandle columnFamilyHandle = getColumnFamilyHandle(
            columnFamilyDefinition.getName().getBytes(UTF_8),
            columnFamilyHandles);
        if (columnFamilyHandle == null) {
          throw new IllegalStateException("columnFamilyHandle is null");
        }

        try (ManagedRocksIterator iterator = new ManagedRocksIterator(
            db.get().newIterator(columnFamilyHandle))) {
          iterator.get().seekToFirst();
          while (!objIds.isEmpty() && iterator.get().isValid()) {
            String subDir = prefix + parentPair.getKey() + OM_KEY_PREFIX;
            String key = new String(iterator.get().key(), UTF_8);
            if (!key.contains(subDir)) {
              iterator.get().next();
              continue;
            }

            OmDirectoryInfo childDir =
                ((OmDirectoryInfo) columnFamilyDefinition.getValueCodec()
                    .fromPersistedFormat(iterator.get().value()));
            Pair<Long, Path> pathVal = Pair.of(childDir.getObjectID(),
                parentPair.getValue().resolve(childDir.getName()));
            addToPathMap(pathVal, objIds, objectIdPathMap);
            objectIdPathVals.add(pathVal);
            iterator.get().next();
          }
        }
      }
    }
    // Invalid directory objectId which does not exist in the given bucket.
    if (!objIds.isEmpty()) {
      throw new IllegalArgumentException(
          "Dir object Ids required but not found in bucket: " + objIds);
    }
    return objectIdPathMap;
  }

  private void addToPathMap(Pair<Long, Path> objectIDPath,
                            Set<Long> dirObjIds, Map<Long, Path> pathMap) {
    if (dirObjIds.contains(objectIDPath.getKey())) {
      pathMap.put(objectIDPath.getKey(), objectIDPath.getValue());
      dirObjIds.remove(objectIDPath.getKey());
    }
  }

  private String removeTrailingSlashIfNeeded(String dbPath) {
    if (dbPath.endsWith(OzoneConsts.OZONE_URI_DELIMITER)) {
      dbPath = dbPath.substring(0, dbPath.length() - 1);
    }
    return dbPath;
  }

  public ColumnFamilyHandle getColumnFamilyHandle(
      byte[] name, List<ColumnFamilyHandle> columnFamilyHandles) {
    return columnFamilyHandles
        .stream()
        .filter(
            handle -> {
              try {
                return Arrays.equals(handle.getName(), name);
              } catch (Exception ex) {
                throw new RuntimeException(ex);
              }
            })
        .findAny()
        .orElse(null);
  }

}
