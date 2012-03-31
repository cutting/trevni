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

package org.apache.trevni.avro;

import java.io.IOException;
import java.io.Closeable;
import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnValues;
import org.apache.trevni.Input;
import org.apache.trevni.InputFile;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;

import static org.apache.trevni.avro.AvroColumnator.isSimple;

/** Random access to files written with {@link AvroColumnWriter}. */
public class AvroColumnReader<D>
  implements Iterator<D>, Iterable<D>, Closeable {

  private ColumnFileReader reader;
  private GenericData model;
  private Schema fullSchema;
  private Schema subSchema;
  
  private ColumnValues[] values;
  private int[] arrayWidths;
  private int column;                          // current index in values

  /** Construct a reader for a file. */
  public AvroColumnReader(File file) throws IOException {
    this(new InputFile(file), GenericData.get());
  }

  /** Construct a reader for a file. */
  public AvroColumnReader(File file, GenericData model) throws IOException {
    this(new InputFile(file), model);
  }

  /** Construct a reader for a file. */
  public AvroColumnReader(Input in, GenericData model)
    throws IOException {
    this.reader = new ColumnFileReader(in);
    this.model = model;
    this.fullSchema =
      Schema.parse(reader.getMetaData().getString(AvroColumnWriter.SCHEMA_KEY));
    setSchema(fullSchema);
  }

  /** Return the schema for data in this file. */
  public Schema getFullSchema() { return fullSchema; }

  /** Set a subset schema for reading.  By default, the full schema. */
  void setSchema(Schema subSchema) throws IOException {
    this.subSchema = subSchema;

    Map<String,Integer> fullColumns = new HashMap<String,Integer>();
    int i = 0;
    for (ColumnMetaData c : new AvroColumnator(fullSchema).getColumns())
      fullColumns.put(c.getName(), i++);

    AvroColumnator subColumnator = new AvroColumnator(subSchema);
    this.arrayWidths = subColumnator.getArrayWidths();
    ColumnMetaData[] subColumns = subColumnator.getColumns();
    this.values = new ColumnValues[subColumns.length];
    int j = 0;
    for (ColumnMetaData c : subColumns) {
      Integer column = fullColumns.get(c.getName());
      if (column == null)
        throw new RuntimeException("No column named: "+c.getName());
      values[j++] = reader.getValues(column);
    }
  }

  @Override
  public Iterator<D> iterator() { return this; }

  @Override
  public boolean hasNext() {
    return values[0].hasNext();
  }

  @Override
  public D next() {
    try {
      for (int i = 0; i < values.length; i++)
        values[i].startRow();
      this.column = 0;
      return (D)read(subSchema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Object read(Schema s) throws IOException {
    if (isSimple(s))
      return values[column++].nextValue();

    switch (s.getType()) {
    case MAP: 
      throw new RuntimeException("Unknown schema: "+s);
    case RECORD: 
      Object record = model.newRecord(null, s);
      for (Field f : s.getFields())
        model.setField(record, f.name(), f.pos(), read(f.schema()));
      return record;
    case ARRAY: 
      int length = values[column].nextLength();
      List elements = (List)new GenericData.Array(length, s);
      int startColumn = column;
      for (int i = 0; i < length; i++) {
        this.column = startColumn;
        Object value = values[column++].nextValue();
        if (!isSimple(s.getElementType()))
          value = read(s.getElementType());
        elements.add(value);
      }
      column = startColumn + arrayWidths[startColumn];
      return elements;
    case UNION:
      Object value = null;
      for (Schema branch : s.getTypes()) {
        if (values[column].nextLength() == 1) {
          value = values[column].nextValue();
          column++;
          if (!isSimple(branch))
            value = read(branch);
        } else {
          column += arrayWidths[column];
        }
      }
      return value;
    default:
      throw new RuntimeException("Unknown schema: "+s);
    }
  }

  @Override
  public void remove() { throw new UnsupportedOperationException(); }

  @Override
  public void close() throws IOException {
    reader.close();
  }

}
