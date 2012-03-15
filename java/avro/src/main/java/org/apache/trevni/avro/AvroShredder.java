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
import java.util.List;
import java.util.ArrayList;

import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ColumnFileWriter;
import org.apache.trevni.ValueType;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;

public class AvroShredder {

  private Schema schema;
  private GenericData data;

  private List<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
  private List<Integer> arrayWidths = new ArrayList<Integer>();

  public AvroShredder(Schema schema, GenericData data) {
    this.schema = schema;
    this.data = data;
    columnize(schema.getFullName(), schema, null, false);
  }

  public ColumnMetaData[] getColumns(Schema s) {
    return columns.toArray(new ColumnMetaData[columns.size()]);
  }

  private void columnize(String name, Schema s,
                         ColumnMetaData parent, boolean isArray) {
    if (isSimple(s)) {
      addColumn(name, simpleValueType(s), parent, isArray);
      return;
    }
    switch (s.getType()) {
    case MAP: 
      throw new RuntimeException("Unknown schema: "+s);
    case RECORD:
      for (Field field : s.getFields())           // flatten fields to columns
        columnize(name+"#"+field.name(), field.schema(), parent, isArray);
      break;
    case ARRAY: 
      addArrayColumn(name, s.getElementType(), parent);
      break;
    case UNION:
      for (Schema branch : s.getTypes())          // array per branch
        addArrayColumn(branch.getFullName(), branch, parent);
      break;
    default:
      throw new RuntimeException("Unknown schema: "+s);
    }
  }

  private ColumnMetaData addColumn(String name, ValueType type,
                                   ColumnMetaData parent, boolean isArray) {
    String path = parent == null ? name : parent.getName()+"#"+name;
    ColumnMetaData column = new ColumnMetaData(path, type);
    if (parent != null)
      column.setParent(parent.getName());
    column.setIsArray(isArray);
    columns.add(column);
    arrayWidths.add(isArray ? 1 : -1);
  }

  private void addArrayColumn(String name, Schema element,
                              ColumnMetaData parent) {
    if (isSimple(element)) {                      // optimize simple arrays
      addColumn(name, simpleValueType(element), parent, true);
      return;
    }
    // complex array: insert a parent column with lengths
    int start = columns.size();
    ColumnMetaData array = addColumn(name, ValueType.NULL, parent, true);
    columnize(name, element, array, false); 
    arrayWidths.set(start, columns.size()-start); // fixup with actual width
  }

  private boolean isSimple(Schema s) {
    switch (s.getType()) {
    case NULL:
    case INT: case LONG:
    case FLOAT: case DOUBLE: 
    case BYTES: case STRING: 
    case ENUM: case FIXED:
      return true;
    default:
      return false;
    }
  }

  private ValueType simpleValueType(Schema s) {
    switch (s.getType()) {
    case NULL:   return ValueType.NULL;
    case INT:    return ValueType.INT;
    case LONG:   return ValueType.LONG;
    case FLOAT:  return ValueType.FLOAT;
    case DOUBLE: return ValueType.DOUBLE;
    case BYTES:  return ValueType.BYTES;
    case STRING: return ValueType.STRING;
    case ENUM:   return ValueType.INT;
    case FIXED:  return ValueType.BYTES;
    default:
      throw new RuntimeException("Unknown schema: "+s);
    }
  }

  public void shred(Object value, ColumnFileWriter writer) throws IOException {
    int count = shred(value, schema, 0, writer);
    assert(count == columns.size());
  }
  
  private int shred(Object o, Schema s, int column,
                    ColumnFileWriter writer) throws IOException {
    if (isSimple(s)) {
      writer.writeValue(o, column);
      return column+1;
    }
    switch (s.getType()) {
    case MAP: 
      throw new RuntimeException("Unknown schema: "+s);
    case RECORD: 
      for (Field f : s.getFields())
        column = shred(data.getField(o, f.name(), f.pos()), f.schema(), column);
      return column;
    case ARRAY: 
      writer.writeLength(o.length(), column);
      if (isSimple(s)) {                          // optimize simple arrays
        for (Object element : o.elements())
          writer.writeValue(element, column);
        return column+1;
      }
      for (Object element : o.elements()) {
        writer.writeValue(null, column);
        shred(o, s.getElementType(), column+1, writer);
      }
      return column+columnWidths[column];
    case UNION:
      int b = data.resolveUnion(s, o);
      int i = 0;
      for (Schema branch : s.getTypes()) {
        boolean selected = i++ == b;
        if (!selected) {
          writer.writeLength(0, column++);
        } else {
          writer.writeLength(1, column);
          if (isSimple(branch)) {
            writer.writeValue(o, column++);
          } else {
            writer.writeValue(null, column);
            column = shred(o, branch, column+1, writer);
          }
        }
      }
      return column;
    default:
      throw new RuntimeException("Unknown schema: "+s);
    }
  }
}    
