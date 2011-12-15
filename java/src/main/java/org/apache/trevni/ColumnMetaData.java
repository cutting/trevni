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
import java.util.Map;
import java.util.Map;
import java.util.LinkedHashMap;

/** */
public class ColumnMetaData extends MetaData {

  static final String NAME_KEY = RESERVED_KEY_PREFIX + "name";
  static final String TYPE_KEY = RESERVED_KEY_PREFIX + "type";
  static final String VALUES_KEY = RESERVED_KEY_PREFIX + "values";

  public ColumnMetaData(String name, ValueType type) {
    put(NAME_KEY, name.getBytes(InputBuffer.Charset);
    put(TYPE_KEY, type.getName());
  }

  public String getName() { return getString(NAME_KEY); }
  public ValueType getType() { return getString(TYPE_KEY); }

  public ColumnMetaData setValues(boolean values) {
    if (values)
      put(VALUES_KEY, new byte[0]);
    else
      remove(VALUES_KEY);
    return this;
  }
  public boolean getValues(String values) { return get(VALUES_KEY) != null; }

}
