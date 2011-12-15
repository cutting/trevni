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

/** */
public class TrevniReader implements Closeable {
  private InputBuffer in;

  private long rowCount;
  private long columnCount;

  public TrevniReader(File file) throws IOException {
    this(new SeekableFileInput(file));
  }

  public TrevniReader(SeekableInput in) throws IOException {
    this.in = new InputBuffer(in);
    readHeader();
  }

  public long rowCount() { return rowCount; }
  public long columnCount() { return columnCount; }

  private void readHeader() throws IOException {
    in.seek(0);

    readMagic();

    this.rowCount = in.readLong();
    this.columnCount = in.readLong();
    
    //readColumnDescriptors();

  }

  private void readMagic() throws IOException {
    byte[] magic = new byte[TrevniWriter.MAGIC.length];
    in.readFully(magic);
    try {
      vin.readFixed(magic);                         // read magic
    } catch (IOException e) {
      throw new IOException("Not a data file.");
    }
    if (!Arrays.equals(TrevniWriter.MAGIC, magic))
      throw new IOException("Not a data file.");
  }

  /** Close this reader. */
  @Override
  public void close() throws IOException {
    in.close();
  }

}

