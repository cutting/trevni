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
  private ColumnDescriptor[] columns;

  public ColumnFileReader(File file) throws IOException {
    this(new InputFile(file));
  }

  public ColumnFileReader(Input file) throws IOException {
    this.file = file;
    readHeader();
  }

  public long getRowCount() { return rowCount; }
  public long getColumnCount() { return columnCount; }

  public ColumnFileMetaData getMetaData() { return metaData; }

  private void readHeader() throws IOException {
    InputBuffer in = new InputBuffer(file, 0);
    readMagic(in);
    this.rowCount = in.readFixed64();
    this.columnCount = in.readFixed32();
    this.metaData = ColumnFileMetaData.read(in);

    columns = new ColumnDescriptor[columnCount];
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
    for (int i = 0; i < columnCount; i++)
      columns[i] = new ColumnDescriptor(file, ColumnMetaData.read(in));
  }

  private void readColumnStarts(InputBuffer in) throws IOException {
    for (int i = 0; i < columnCount; i++)
      columns[i].start = in.readFixed64();
  }
 
  public <T> ColumnValues<T> getValues(final int column) throws IOException {
    return new ColumnValues<T>(columns[column]);
  }

  /** Close this reader. */
  @Override
  public void close() throws IOException {
    file.close();
  }

}

