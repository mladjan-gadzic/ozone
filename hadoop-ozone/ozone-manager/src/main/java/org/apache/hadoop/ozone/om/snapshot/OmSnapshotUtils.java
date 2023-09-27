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

package org.apache.hadoop.ozone.om.snapshot;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.om.OmSnapshotManager;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.OzoneConsts.OM_CHECKPOINT_DIR;

/**
 * Ozone Manager Snapshot Utilities.
 */
public final class OmSnapshotUtils {

  private OmSnapshotUtils() { }

  /**
   * Get the filename without the introductory metadata directory.
   *
   * @param truncateLength Length to remove.
   * @param file           File to remove prefix from.
   * @return Truncated string.
   */
  public static String truncateFileName(int truncateLength, Path file) {
    return file.toString().substring(truncateLength);
  }

  /**
   * Get the INode for file.
   *
   * @param file File whose INode is to be retrieved.
   * @return INode for file.
   */
  @VisibleForTesting
  public static Object getINode(Path file) throws IOException {
    return Files.readAttributes(file, BasicFileAttributes.class).fileKey();
  }

  /**
   * Create file of links to add to tarball.
   * Format of entries are either:
   * dir1/fileTo fileFrom
   * for files in active db or:
   * dir1/fileTo dir2/fileFrom
   * for files in another directory, (either another snapshot dir or
   * sst compaction backup directory)
   *
   * @param truncateLength - Length of initial path to trim in file path.
   * @param hardLinkFiles  - Map of link->file paths.
   * @return Path to the file of links created.
   */
  public static Path createHardLinkList(int truncateLength,
                                        Map<Path, Path> hardLinkFiles)
      throws IOException {
    Path data = Files.createTempFile("data", "txt");
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Path, Path> entry : hardLinkFiles.entrySet()) {
      String fixedFile = truncateFileName(truncateLength, entry.getValue());
      // If this file is from the active db, strip the path.
      if (fixedFile.startsWith(OM_CHECKPOINT_DIR)) {
        Path f = Paths.get(fixedFile).getFileName();
        if (f != null) {
          fixedFile = f.toString();
        }
      }
      sb.append(truncateFileName(truncateLength, entry.getKey())).append("\t")
          .append(fixedFile).append("\n");
    }
    Files.write(data, sb.toString().getBytes(StandardCharsets.UTF_8));
    return data;
  }

  /**
   * Create hard links listed in OM_HARDLINK_FILE.
   *
   * @param dbPath Path to db to have links created.
   */
  public static void createHardLinks(Path dbPath) throws IOException {
    File hardLinkFile =
        new File(dbPath.toString(), OmSnapshotManager.OM_HARDLINK_FILE);

    if (!hardLinkFile.exists()) {
      return;
    }

    // Read file.
    try (Stream<String> lines = Files.lines(hardLinkFile.toPath()).parallel()) {
      AtomicInteger failureCount = new AtomicInteger(0);

      lines
          .map(line -> line.split("\t"))
          .filter(parts -> parts.length == 2)
          .forEach(parts -> {
            try {
              String from = parts[1];
              String to = parts[0];
              Path fullFromPath = dbPath.resolve(from);
              Path fullToPath = dbPath.resolve(to);

              // Make parent dir if it doesn't exist.
              Path parent = fullToPath.getParent();
              if (Objects.nonNull(parent) && !Files.exists(parent)) {
                Files.createDirectories(parent);
              }

              Files.createLink(fullToPath, fullFromPath);
            } catch (IOException e) {
              failureCount.incrementAndGet();
            }
          });

      if (failureCount.get() != 0) {
        throw new IOException("Failed to read file: " + hardLinkFile);
      }

      // Delete the hard link file if processing is successful
      if (!hardLinkFile.delete()) {
        throw new IOException("Failed to delete: " + hardLinkFile);
      }
    }
  }

  /**
   * Link each of the files in oldDir to newDir.
   *
   * @param oldDir The dir to create links from.
   * @param newDir The dir to create links to.
   */
  public static void linkFiles(File oldDir, File newDir) throws IOException {
    int truncateLength = oldDir.toString().length() + 1;
    List<String> oldDirList;
    try (Stream<Path> files = Files.walk(oldDir.toPath())) {
      oldDirList = files.map(Path::toString).
          // Don't copy the directory itself
          filter(s -> !s.equals(oldDir.toString())).
          // Remove the old path
          map(s -> s.substring(truncateLength)).
          sorted().
          collect(Collectors.toList());
    }
    for (String s: oldDirList) {
      File oldFile = new File(oldDir, s);
      File newFile = new File(newDir, s);
      File newParent = newFile.getParentFile();
      if (!newParent.exists()) {
        if (!newParent.mkdirs()) {
          throw new IOException("Directory create fails: " + newParent);
        }
      }
      if (oldFile.isDirectory()) {
        if (!newFile.mkdirs()) {
          throw new IOException("Directory create fails: " + newFile);
        }
      } else {
        Files.createLink(newFile.toPath(), oldFile.toPath());
      }
    }
  }
}
