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
import java.io.Closeable;
import java.io.File;
import java.util.Arrays;

/** */
public class ColumnFileReader implements Closeable {
  private Input file;

  private long rowCount;
  private int columnCount;
  private ColumnFileMetaData metaData;

  private ColumnMetaData[] columns;
  private long[] starts;

  private static class Block {
    int valueCount;
    int uncompressedSize;
    int compressedSize;

    private Block() {}                            // private ctor

    public static Block read(InputBuffer in) throws IOException {
      Block result = new Block();
      result.valueCount = in.readFixed32();
      result.uncompressedSize = in.readFixed32();
      result.compressedSize = in.readFixed32();
      return result;
    }
  }

  private Block[][] blocks;

  public ColumnFileReader(File file) throws IOException {
    this(new InputFile(file));
  }

  public ColumnFileReader(Input file) throws IOException {
    this.file = file;
    readHeader();
    blocks = new Block[columnCount][];
  }

  public long rowCount() { return rowCount; }
  public long columnCount() { return columnCount; }

  public ColumnFileMetaData getMetaData() { return metaData; }

  private void readHeader() throws IOException {
    InputBuffer in = new InputBuffer(file, 0);
    readMagic(in);
    this.rowCount = in.readFixed64();
    this.columnCount = in.readFixed32();
    this.metaData = ColumnFileMetaData.read(in);
    readColumnMetaData(in);
    readColumnStarts(in);
  }

  private void readMagic(InputBuffer in) throws IOException {
    byte[] magic = new byte[ColumnFileWriter.MAGIC.length];
    try {
      in.readFully(magic);
    } catch (IOException e) {
      throw new IOException("Not a data file.");
    }
    if (!Arrays.equals(ColumnFileWriter.MAGIC, magic))
      throw new IOException("Not a data file.");
  }

  private void readColumnMetaData(InputBuffer in) throws IOException {
    columns = new ColumnMetaData[columnCount];
    for (int i = 0; i < columnCount; i++)
      columns[i] = ColumnMetaData.read(in);
  }

  private void readColumnStarts(InputBuffer in) throws IOException {
    starts = new long[columnCount];
    for (int i = 0; i < columnCount; i++)
      starts[i] = in.readFixed64();
  }
 
  // Block getBlock(int column, long row) {
  //   Block[] columnBlocks = blocks[column];
  //   if (columnBlocks = null)
  //     readColumBlocks(column)
  // }
  // Block getBlock(int column, byte[] key);
  
  private Block[] getColumnBlocks(int column) throws IOException {
    Block[] result = blocks[column];
    if (result == null)
      result = readColumnBlocks(column);
    return result;
  }

  private Block[] readColumnBlocks(int column) throws IOException {
    InputBuffer in = new InputBuffer(file, starts[column]);
    int blockCount = in.readFixed32();
    Block[] result = new Block[blockCount];
    for (int i = 0; i < blockCount; i++)
      result[i] = Block.read(in);
    return result;
  }

  /** Close this reader. */
  @Override
  public void close() throws IOException {
    file.close();
  }

}

