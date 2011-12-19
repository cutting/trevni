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
public class ColumnMetaData extends MetaData {

  static final String NAME_KEY = RESERVED_KEY_PREFIX + "name";
  static final String TYPE_KEY = RESERVED_KEY_PREFIX + "type";
  static final String VALUES_KEY = RESERVED_KEY_PREFIX + "values";

  // cache these values for better performance
  private String name;
  private ValueType type;
  private boolean values;

  private ColumnMetaData() {}                     // non-public ctor
  public ColumnMetaData(String name, ValueType type) {
    this.name = name;
    setReserved(NAME_KEY, name);
    this.type = type;
    setReserved(TYPE_KEY, type.getName());
  }

  public String getName() { return name; }
  public ValueType getType() { return type; }

  public ColumnMetaData setValues(boolean values) {
    this.values = values;
    if (values)
      setReserved(VALUES_KEY, "");
    else
      remove(VALUES_KEY);
    return this;
  }
  public boolean getValues(String values) { return get(VALUES_KEY) != null; }

  static ColumnMetaData read(InputBuffer in) throws IOException {
    ColumnMetaData result = new ColumnMetaData();
    MetaData.read(in, result);
    result.name = result.getString(NAME_KEY);
    result.type = ValueType.forName(result.getString(TYPE_KEY));
    result.values = result.get(VALUES_KEY) != null;
    return result;
  }

}
