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
import java.util.Collection;
import java.util.Map;
import java.util.IdentityHashMap;

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
    columnize(null, schema, null, false);
  }

  public ColumnMetaData[] getColumns() {
    return columns.toArray(new ColumnMetaData[columns.size()]);
  }

  private Map<Schema,Schema> seen = new IdentityHashMap<Schema,Schema>();

  private void columnize(String path, Schema s,
                         ColumnMetaData parent, boolean isArray) {

    if (isSimple(s)) {
      if (path == null) path = s.getFullName();
      addColumn(path, simpleValueType(s), parent, isArray);
      return;
    }

    if (seen.containsKey(s))                      // catch recursion
      throw new RuntimeException("Cannot shred recursive schemas: "+s);
    seen.put(s, s);
    
    switch (s.getType()) {
    case MAP: 
      throw new RuntimeException("Can't shred maps yet: "+s);
    case RECORD:
      for (Field field : s.getFields())           // flatten fields to columns
        columnize(p(path, field.name()), field.schema(), parent, isArray);
      break;
    case ARRAY: 
      if (parent != null)
        path = p(parent.getName(), s.getElementType().getFullName());
      addArrayColumn(path, s.getElementType(), parent);
      break;
    case UNION:
      for (Schema branch : s.getTypes())          // array per branch
        addArrayColumn(p(path, branch.getFullName()), branch, parent);
      break;
    default:
      throw new RuntimeException("Unknown schema: "+s);
    }
  }

  private String p(String parent, String child) {
    return parent == null ? child : parent + "#" + child;
  }

  private ColumnMetaData addColumn(String path, ValueType type,
                                   ColumnMetaData parent, boolean isArray) {
    ColumnMetaData column = new ColumnMetaData(path, type);
    if (parent != null)
      column.setParent(parent);
    column.isArray(isArray);
    columns.add(column);
    arrayWidths.add(isArray ? 1 : -1);
    return column;
 }

  private void addArrayColumn(String path, Schema element,
                              ColumnMetaData parent) {
    if (path == null) path = element.getFullName();
    if (isSimple(element)) {                      // optimize simple arrays
      addColumn(path, simpleValueType(element), parent, true);
      return;
    }
    // complex array: insert a parent column with lengths
    int start = columns.size();
    ColumnMetaData array = addColumn(path, ValueType.NULL, parent, true);
    columnize(path, element, array, false); 
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
    writer.startRow();
    int count = shred(value, schema, 0, writer);
    assert(count == columns.size());
    writer.endRow();
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
        column = shred(data.getField(o, f.name(), f.pos()), f.schema(),
                       column, writer);
      return column;
    case ARRAY: 
      Collection elements = (Collection)o;
      writer.writeLength(elements.size(), column);
      if (isSimple(s.getElementType())) {              // optimize simple arrays
        for (Object element : elements)
          writer.writeValue(element, column);
        return column+1;
      }
      for (Object element : elements) {
        writer.writeValue(null, column);
        int c = shred(element, s.getElementType(), column+1, writer);
        assert(c == column+arrayWidths.get(column));
      }
      return column+arrayWidths.get(column);
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
