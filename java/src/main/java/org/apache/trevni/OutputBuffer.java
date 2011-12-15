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

import java.io.EOFException;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/** */

class OutputBuffer extends ByteArrayOutputStream {
  private long valueCount;

  public OutputBuffer() {
    super(TrevniWriter.BLOCK_SIZE + TrevniWriter.BLOCK_SIZE >> 3);
  }

  public long getValueCount() { return valueCount; }

  public boolean isFull() { return size() >= TrevniWriter.BLOCK_SIZE; }

  public void writeValue(Object value, ColumnMetaData column)
    throws IOException {
    switch (column.getType()) {
    case INT:
      writeInt((Integer)value);               break;
    case LONG:
      writeLong((Long)value);                 break;
    case FIXED32:
      writeFixed32((Integer)value);           break;
    case FIXED64:
      writeFixed64((Long)value);              break;
    case FLOAT:
      writeFloat((Float)value);               break;
    case DOUBLE:
      writeDouble((Double)value);             break;
    case STRING:
      writeString((String)value);             break;
    case BYTES:
      if (value instanceof ByteBuffer)
        writeBytes((ByteBuffer)value);
      else
        writeBytes((byte[])value);
      break;
    default:
      throw new TrevniRuntimeException("Unknown value type: "+column.getType());
    }
    valueCount++;
  }

  public void writeString(String string) throws IOException {
    if (0 == string.length()) {
      write(0);
      return;
    }
    byte[] bytes = string.getBytes("UTF-8");
    writeInt(bytes.length);
    write(bytes, 0, bytes.length);
  }

  public void writeBytes(ByteBuffer bytes) throws IOException {
    int pos = bytes.position();
    int start = bytes.arrayOffset() + pos;
    int len = bytes.limit() - pos;
    writeBytes(bytes.array(), start, len);
  }
  
  public void writeBytes(byte[] bytes) throws IOException {
    writeBytes(bytes, 0, bytes.length);
  }

  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    if (0 == len) {
      write(0);
      return;
    }
    writeInt(len);
    write(bytes, start, len);
  }

  public void writeFloat(float f) throws IOException {
    writeFixed32(Float.floatToRawIntBits(f));
  }

  public void writeDouble(double d) throws IOException {
    writeFixed64(Double.doubleToRawLongBits(d));
  }

  private final byte[] bytes = new byte[12];
  
  public void writeFixed32(int i) throws IOException {
    bytes[0] = (byte)((i       ) & 0xFF);
    bytes[1] = (byte)((i >>>  8) & 0xFF);
    bytes[2] = (byte)((i >>> 16) & 0xFF);
    bytes[3] = (byte)((i >>> 24) & 0xFF);
    write(bytes, 0, 4);
  }

  public void writeFixed64(long l) throws IOException {
    int first = (int)(l & 0xFFFFFFFF);
    int second = (int)((l >>> 32) & 0xFFFFFFFF);
    bytes[0] = (byte)((first        ) & 0xFF);
    bytes[4] = (byte)((second       ) & 0xFF);
    bytes[5] = (byte)((second >>>  8) & 0xFF);
    bytes[1] = (byte)((first >>>   8) & 0xFF);
    bytes[2] = (byte)((first >>>  16) & 0xFF);
    bytes[6] = (byte)((second >>> 16) & 0xFF);
    bytes[7] = (byte)((second >>> 24) & 0xFF);
    bytes[3] = (byte)((first >>>  24) & 0xFF);
    write(bytes, 0, 8);
  }

  public void writeInt(int n) throws IOException {
    n = (n << 1) ^ (n >> 31);                     // move sign to low-order bit
    if ((n & ~0x7F) == 0) {                       // optimize one-byte case
      write(n);
      return;
    } else if ((n & ~0x3FFF) == 0) {              // optimize two-byte case
      write(0x80 | n);
      write(n >>> 7);
      return;
    }
    int start = pos;                              // unroll general case
    if ((n & ~0x7F) != 0) {
      bytes[pos++] = (byte)((n | 0x80) & 0xFF);
      n >>>= 7;
      if (n > 0x7F) {
        bytes[pos++] = (byte)((n | 0x80) & 0xFF);
        n >>>= 7;
        if (n > 0x7F) {
          bytes[pos++] = (byte)((n | 0x80) & 0xFF);
          n >>>= 7;
          if (n > 0x7F) {
            bytes[pos++] = (byte)((n | 0x80) & 0xFF);
            n >>>= 7;
          }
        }
      }
    } 
    bytes[pos++] = (byte) n;
    write(bytes, 0, pos - start);
  }

  public void writeLong(long n) throws IOException {
    n = (n << 1) ^ (n >> 63);                     // move sign to low-order bit
    if ((n & ~0x7FFFFFFFL) == 0) {                // optimize 32-bit case
      int i = (int) n;
      while ((i & ~0x7F) != 0) {
        write((byte)((0x80 | i) & 0xFF));
        i >>>= 7;
      }
      write((byte)i);
      return;
    }
    int start = pos;                              // unroll general case
    if ((n & ~0x7FL) != 0) {
      bytes[pos++] = (byte)((n | 0x80) & 0xFF);
      n >>>= 7;
      if (n > 0x7F) {
        bytes[pos++] = (byte)((n | 0x80) & 0xFF);
        n >>>= 7;
        if (n > 0x7F) {
          bytes[pos++] = (byte)((n | 0x80) & 0xFF);
          n >>>= 7;
          if (n > 0x7F) {
            bytes[pos++] = (byte)((n | 0x80) & 0xFF);
            n >>>= 7;
            if (n > 0x7F) {
              bytes[pos++] = (byte)((n | 0x80) & 0xFF);
              n >>>= 7;
              if (n > 0x7F) {
                bytes[pos++] = (byte)((n | 0x80) & 0xFF);
                n >>>= 7;
                if (n > 0x7F) {
                  bytes[pos++] = (byte)((n | 0x80) & 0xFF);
                  n >>>= 7;
                  if (n > 0x7F) {
                    bytes[pos++] = (byte)((n | 0x80) & 0xFF);
                    n >>>= 7;
                    if (n > 0x7F) {
                      bytes[pos++] = (byte)((n | 0x80) & 0xFF);
                      n >>>= 7;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    bytes[pos++] = (byte) n;
    write(bytes, 0, pos - start);
  }
  
}
