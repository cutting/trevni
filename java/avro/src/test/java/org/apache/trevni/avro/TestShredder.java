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

import org.apache.trevni.ValueType;
import org.apache.trevni.ColumnMetaData;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestShredder {

  private static final String R1 =
    "{\"type\":\"record\",\"name\":\"Foo\",\"fields\":["
    +"{\"name\":\"x\",\"type\":\"int\"},"
    +"{\"name\":\"y\",\"type\":\"string\"}"
    +"]}";

  @Test public void testPrimitiveColumns() throws Exception {
    checkColumns(Schema.create(Schema.Type.INT),
                 new ColumnMetaData("int", ValueType.INT));
  }

  @Test public void testSimpleRecordColumns() throws Exception {
    checkColumns(Schema.parse(R1),
                 new ColumnMetaData("x", ValueType.INT),
                 new ColumnMetaData("y", ValueType.STRING));
  }

  @Test public void testSimpleUnionColumns() throws Exception {
    String s = "[\"int\",\"string\"]";
    checkColumns(Schema.parse(s),
                 new ColumnMetaData("int", ValueType.INT).isArray(true),
                 new ColumnMetaData("string", ValueType.STRING).isArray(true));
  }

  @Test public void testUnionColumns() throws Exception {
    String s = "[\"int\","+R1+"]";
    ColumnMetaData p =
      new ColumnMetaData("Foo", ValueType.NULL).isArray(true);
    checkColumns(Schema.parse(s),
                 new ColumnMetaData("int", ValueType.INT).isArray(true),
                 p,
                 new ColumnMetaData("Foo#x", ValueType.INT).setParent(p),
                 new ColumnMetaData("Foo#y", ValueType.STRING).setParent(p));
  }

  private void checkColumns(Schema s, ColumnMetaData... expected) {
    ColumnMetaData[] shredded =
      new AvroShredder(s, GenericData.get()).getColumns();
    assertEquals(expected.length, shredded.length);
    for (int i = 0; i < expected.length; i++)
      assertEquals(expected[i].toString(), shredded[i].toString());
  }

}
