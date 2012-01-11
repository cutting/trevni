/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.trevni;

import java.io.IOException;

/** */
class ColumnDescriptor {
  final Input file;
  final ColumnMetaData metaData;

  long start;
  long dataStart;

  private BlockDescriptor[] blocks;

  public ColumnDescriptor(Input file, ColumnMetaData metaData) {
    this.file = file;
    this.metaData = metaData;
  }

  public BlockDescriptor[] getBlocks() throws IOException {
    if (blocks == null)
      blocks = readBlocks();
    return blocks;
  }

  private BlockDescriptor[] readBlocks() throws IOException {
    // read block descriptors
    InputBuffer in = new InputBuffer(file, start);
    int blockCount = in.readFixed32();
    BlockDescriptor[] result = new BlockDescriptor[blockCount];
    for (int i = 0; i < blockCount; i++)
      result[i] = BlockDescriptor.read(in);
    dataStart = in.tell();

    // add positions and rows to block descriptors
    long startPosition = dataStart;
    long row = 0;
    for (int i = 0; i < blockCount; i++) {
      BlockDescriptor b = result[i];
      b.startPosition = startPosition;
      b.firstRow = row;
      startPosition += b.uncompressedSize; //FIXME: add checksum size
      row += b.rowCount;
    }
    return result;
  }

}
