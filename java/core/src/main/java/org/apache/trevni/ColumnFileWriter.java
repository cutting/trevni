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
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

/** Writes data to a column file.
 * All data is buffered until {@link #writeTo(File)} is called.
 */
public class ColumnFileWriter {

  static final byte[] MAGIC = new byte[] {'T', 'r', 'v', 0};

  private ColumnFileMetaData metaData;
  private ColumnOutputBuffer[] columns;

  private long rowCount;
  private int columnCount;

  /** Construct given metadata for each column in the file. */
  public ColumnFileWriter(ColumnFileMetaData fileMeta,
                          ColumnMetaData... columnMeta) throws IOException {
    this.metaData = fileMeta;
    this.columnCount = columnMeta.length;
    this.columns = new ColumnOutputBuffer[columnCount];
    for (int i = 0; i < columnCount; i++) {
      columnMeta[i].setDefaults(metaData);
      columns[i] = new ColumnOutputBuffer(columnMeta[i]);
    }
  }

  /** Return this file's metadata. */
  public ColumnFileMetaData getMetaData() { return metaData; }

  /** Add a row to the file. */
  public void writeRow(Object... row) throws IOException {
    for (int column = 0; column < columnCount; column++)
      columns[column].writeValue(row[column]);
    rowCount++;
  }

  public void writeValue(Object value, int column) throws IOException {}
  public void writeLength(int length, int column) throws IOException {}


  /** Write all rows added to the named file. */
  public void writeTo(File file) throws IOException {
    OutputStream out = new FileOutputStream(file);
    try {
      writeTo(out);
    } finally {
      out.close();
    }
  }

  /** Write all rows added to the named output stream. */
  public void writeTo(OutputStream out) throws IOException {
    writeHeader(out);
    
    for (int column = 0; column < columnCount; column++)
      columns[column].writeTo(out);
  }

  private void writeHeader(OutputStream out) throws IOException {
    OutputBuffer header = new OutputBuffer();

    header.write(MAGIC);                          // magic

    header.writeFixed64(rowCount);                // row count

    header.writeFixed32(columnCount);             // column count

    metaData.write(header);                       // file metadata

    for (ColumnOutputBuffer column : columns)
      column.getMeta().write(header);             // column metadata

    for (long start : computeStarts(header.size()))
      header.writeFixed64(start);                 // column starts

    header.writeTo(out);

  }

  private long[] computeStarts(long start) throws IOException {
    long[] result = new long[columnCount];
    start += columnCount * 8;                     // room for starts
    for (int column = 0; column < columnCount; column++) {
      result[column] = start;
      start += columns[column].size();
    }
    return result;
  }

}

