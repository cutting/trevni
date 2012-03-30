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

import java.io.File;
import java.io.IOException;

import org.apache.trevni.ValueType;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ColumnFileMetaData;

import org.apache.avro.Schema;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestShredder {

  private static final int COUNT = 1;
  private static final File FILE = new File("target", "test.trv");
  private static final long SEED = System.currentTimeMillis();

  @Test public void testPrimitive() throws Exception {
    check(Schema.create(Schema.Type.INT),
          new ColumnMetaData("int", ValueType.INT));
  }

  private static final String SIMPLE_RECORD =
    "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
    +"{\"name\":\"x\",\"type\":\"int\"},"
    +"{\"name\":\"y\",\"type\":\"string\"}"
    +"]}";

  @Test public void testSimpleRecord() throws Exception {
    check(Schema.parse(SIMPLE_RECORD),
          new ColumnMetaData("x", ValueType.INT),
          new ColumnMetaData("y", ValueType.STRING));
  }

  @Test public void testNestedRecord() throws Exception {
    String s = 
      "{\"type\":\"record\",\"name\":\"S\",\"fields\":["
      +"{\"name\":\"x\",\"type\":\"int\"},"
      +"{\"name\":\"R\",\"type\":"+SIMPLE_RECORD+"},"
      +"{\"name\":\"y\",\"type\":\"string\"}"
      +"]}";
    check(Schema.parse(s),
          new ColumnMetaData("x", ValueType.INT),
          new ColumnMetaData("R#x", ValueType.INT),
          new ColumnMetaData("R#y", ValueType.STRING),
          new ColumnMetaData("y", ValueType.STRING));
  }

  @Test public void testSimpleArray() throws Exception {
    String s = "{\"type\":\"array\",\"items\":\"long\"}";
    check(Schema.parse(s),
          new ColumnMetaData("long", ValueType.LONG).isArray(true));
  }

  private static final String RECORD_ARRAY = 
    "{\"type\":\"array\",\"items\":"+SIMPLE_RECORD+"}";

  @Test public void testArray() throws Exception {
    ColumnMetaData p = new ColumnMetaData("R", ValueType.NULL).isArray(true);
    check(Schema.parse(RECORD_ARRAY),
          p,
          new ColumnMetaData("R#x", ValueType.INT).setParent(p),
          new ColumnMetaData("R#y", ValueType.STRING).setParent(p));
  }

  @Test public void testSimpleUnion() throws Exception {
    String s = "[\"int\",\"string\"]";
    check(Schema.parse(s),
          new ColumnMetaData("int", ValueType.INT).isArray(true),
          new ColumnMetaData("string", ValueType.STRING).isArray(true));
  }

  private static final String UNION = "[\"int\","+SIMPLE_RECORD+"]";

  @Test public void testUnion() throws Exception {
    ColumnMetaData p = new ColumnMetaData("R", ValueType.NULL).isArray(true);
    check(Schema.parse(UNION),
          new ColumnMetaData("int", ValueType.INT).isArray(true),
          p,
          new ColumnMetaData("R#x", ValueType.INT).setParent(p),
          new ColumnMetaData("R#y", ValueType.STRING).setParent(p));
  }

  @Test public void testNestedArray() throws Exception {
    String s = 
      "{\"type\":\"record\",\"name\":\"S\",\"fields\":["
      +"{\"name\":\"x\",\"type\":\"int\"},"
      +"{\"name\":\"R\",\"type\":"+RECORD_ARRAY+"},"
      +"{\"name\":\"y\",\"type\":\"string\"}"
      +"]}";
    ColumnMetaData p = new ColumnMetaData("R", ValueType.NULL).isArray(true);
    check(Schema.parse(s),
          new ColumnMetaData("x", ValueType.INT),
          p,
          new ColumnMetaData("R#x", ValueType.INT).setParent(p),
          new ColumnMetaData("R#y", ValueType.STRING).setParent(p),
          new ColumnMetaData("y", ValueType.STRING));
  }

  @Test public void testNestedUnion() throws Exception {
    String s = 
      "{\"type\":\"record\",\"name\":\"S\",\"fields\":["
      +"{\"name\":\"x\",\"type\":\"int\"},"
      +"{\"name\":\"u\",\"type\":"+UNION+"},"
      +"{\"name\":\"y\",\"type\":\"string\"}"
      +"]}";
    ColumnMetaData p = new ColumnMetaData("u#R", ValueType.NULL).isArray(true);
    check(Schema.parse(s),
          new ColumnMetaData("x", ValueType.INT),
          new ColumnMetaData("u#int", ValueType.INT).isArray(true),
          p,
          new ColumnMetaData("u#R#x", ValueType.INT).setParent(p),
          new ColumnMetaData("u#R#y", ValueType.STRING).setParent(p),
          new ColumnMetaData("y", ValueType.STRING));
  }

  @Test public void testUnionInArray() throws Exception {
    String s = 
      "{\"type\":\"record\",\"name\":\"S\",\"fields\":["
      +"{\"name\":\"a\",\"type\":{\"type\":\"array\",\"items\":"+UNION+"}}"
      +"]}";
    ColumnMetaData p = new ColumnMetaData("a",ValueType.NULL).isArray(true);
    ColumnMetaData r = new ColumnMetaData("a#R", ValueType.NULL)
      .setParent(p)
      .isArray(true);
      check(Schema.parse(s),
          p,
          new ColumnMetaData("a#int", ValueType.INT)
            .setParent(p)
            .isArray(true),
          r,
          new ColumnMetaData("a#R#x", ValueType.INT).setParent(r),
          new ColumnMetaData("a#R#y", ValueType.STRING).setParent(r));
  }

  @Test public void testArrayInUnion() throws Exception {
    String s = 
      "{\"type\":\"record\",\"name\":\"S\",\"fields\":["
      +"{\"name\":\"a\",\"type\":[\"int\","+RECORD_ARRAY+"]}]}";
    ColumnMetaData q = new ColumnMetaData("a#array",ValueType.NULL)
      .isArray(true);
    ColumnMetaData r = new ColumnMetaData("a#array#R", ValueType.NULL)
      .setParent(q)
      .isArray(true);
    check(Schema.parse(s),
          new ColumnMetaData("a#int", ValueType.INT).isArray(true),
          q,
          r,
          new ColumnMetaData("a#array#R#x", ValueType.INT).setParent(r),
          new ColumnMetaData("a#array#R#y", ValueType.STRING).setParent(r));
  }

  private void check(Schema s, ColumnMetaData... expected) throws Exception {
    ColumnMetaData[] shredded = new AvroColumnator(s).getColumns();
    assertEquals(expected.length, shredded.length);
    for (int i = 0; i < expected.length; i++)
      assertEquals(expected[i].toString(), shredded[i].toString());
    checkWrite(s);
    checkRead(s);
  }

  private void checkWrite(Schema schema) throws IOException {
    AvroColumnWriter<Object> writer =
      new AvroColumnWriter<Object>(schema, new ColumnFileMetaData());
    int count = 0;
    for (Object datum : new RandomData(schema, COUNT, SEED))
      writer.write(datum);
    writer.writeTo(FILE);
  }

  private void checkRead(Schema schema) throws IOException {
    AvroColumnReader<Object> reader = new AvroColumnReader<Object>(FILE);
    for (Object expected : new RandomData(schema, COUNT, SEED))
      assertEquals(expected, reader.next());
    reader.close();
  }

}
