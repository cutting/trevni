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
package org.apache.trevni.tool;

import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ColumnValues;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.util.MinimalPrettyPrinter;

/** Reads a Trevni file and render it as JSON.*/
public class ToJsonTool implements Tool {
  static final JsonFactory FACTORY = new JsonFactory();

  private JsonGenerator generator;
  private ColumnFileReader reader;
  private Map<String,ColumnValues> values;

  @Override
  public String getName() {
    return "tojson";
  }

  @Override
  public String getShortDescription() {
    return "Dumps a Trevni file as JSON.";
  }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err,
                 List<String> args) throws Exception {
    boolean pretty = false;
    if (args.size() == 2 && "-pretty".equals(args.get(1)))
      pretty = true;
    else if (args.size() != 1) {
      err.println("Usage: input [-pretty]");
      return 1;
    }

    this.generator = FACTORY.createJsonGenerator(out, JsonEncoding.UTF8);
    if (pretty) {
      generator.useDefaultPrettyPrinter();
    } else {
      // ensure newline separation
      MinimalPrettyPrinter pp = new MinimalPrettyPrinter();
      pp.setRootValueSeparator(System.getProperty("line.separator"));
      generator.setPrettyPrinter(pp);
    }

    this.reader = new ColumnFileReader(Util.input(args.get(0)));

    this.values = new HashMap<String,ColumnValues>();
    for (ColumnMetaData c : reader.getColumnMetaData())
      values.put(c.getName(), reader.getValues(c.getName()));

    List<ColumnMetaData> roots = reader.getRoots();
    for (long row = 0; row < reader.getRowCount(); row++) {
      for (ColumnValues in : values.values())
        in.startRow();
      generator.writeStartObject();
      for (ColumnMetaData root : roots)
        valueToJson(root);
      generator.writeEndObject();
    }
    generator.flush();
    out.println();
    reader.close();
    return 0;
  }

  private void valueToJson(ColumnMetaData column) throws IOException {
    generator.writeFieldName(shortColumnName(column.getName()));
    ColumnValues in = values.get(column.getName());
    if (!column.isArray()) {
      primitiveToJson(column, in.nextValue());
    } else {
      generator.writeStartArray();
      int length = in.nextLength();
      for (int i = 0; i < length; i++) {
        Object value = in.nextValue();
        List<ColumnMetaData> children = column.getChildren();
        if (children.size() == 0) {
          primitiveToJson(column, value);
        } else {
          generator.writeStartObject();
          if (value != null) {
            generator.writeFieldName("value$");
            primitiveToJson(column, value);
          }
          for (ColumnMetaData child : children)
            valueToJson(child);
          generator.writeEndObject();
        }
      }
      generator.writeEndArray();
    }
  }

  private void primitiveToJson(ColumnMetaData column, Object value) 
    throws IOException {
    switch (column.getType()) {
    case NULL:
      generator.writeNull();                        break;
    case INT:
      generator.writeNumber((Integer)value);        break;
    case LONG:
      generator.writeNumber((Long)value);           break;
    case FIXED32:
      generator.writeNumber((Integer)value);        break;
    case FIXED64:
      generator.writeNumber((Long)value);           break;
    case FLOAT:
      generator.writeNumber((Float)value);          break;
    case DOUBLE:
      generator.writeNumber((Double)value);         break;
    case STRING:
      generator.writeString((String)value);         break;
    case BYTES:
      generator.writeBinary((byte[])value);
      break;
    default:
      throw new RuntimeException("Unknown value type: "+column.getType());
    }
  }

  private String shortColumnName(String name) {
    int lastHash = name.lastIndexOf('#');
    if (lastHash != -1)
      name = name.substring(lastHash+1, name.length());

    int lastDot = name.lastIndexOf('.');
    if (lastDot != -1)
      name = name.substring(lastDot+1, name.length());

    return name;
  }

}
