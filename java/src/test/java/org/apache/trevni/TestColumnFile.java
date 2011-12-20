/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.trevni;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

public class TestColumnFile {

  private static final File FILE = new File("target", "test.trv");
  private static final int COUNT = 1024*128;

  @Test public void testEmpty() throws Exception {
    FILE.delete();
    ColumnFileWriter out = new ColumnFileWriter();
    out.writeTo(FILE);
    ColumnFileReader in = new ColumnFileReader(FILE);
    Assert.assertEquals(0, in.getRowCount());
    Assert.assertEquals(0, in.getColumnCount());
    in.close();
  }

  @Test public void testInts() throws Exception {
    FILE.delete();

    ColumnFileWriter out =
      new ColumnFileWriter(new ColumnMetaData("test", ValueType.INT));
    Random random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      out.writeRow(TestUtil.randomLength(random));
    out.writeTo(FILE);

    random = TestUtil.createRandom();
    ColumnFileReader in = new ColumnFileReader(FILE);
    Assert.assertEquals(COUNT, in.getRowCount());
    Assert.assertEquals(1, in.getColumnCount());
    Iterator<Integer> i = in.getValues(0);
    int count = 0;
    while (i.hasNext()) {
      Assert.assertEquals(TestUtil.randomLength(random), (int)i.next());
      count++;
    }
    Assert.assertEquals(COUNT, count);
  }

  @Test public void testLongs() throws Exception {
    FILE.delete();

    ColumnFileWriter out =
      new ColumnFileWriter(new ColumnMetaData("test", ValueType.LONG));
    Random random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      out.writeRow(random.nextLong());
    out.writeTo(FILE);

    random = TestUtil.createRandom();
    ColumnFileReader in = new ColumnFileReader(FILE);
    Assert.assertEquals(COUNT, in.getRowCount());
    Assert.assertEquals(1, in.getColumnCount());
    Iterator<Long> i = in.getValues(0);
    int count = 0;
    while (i.hasNext()) {
      Assert.assertEquals(random.nextLong(), (long)i.next());
      count++;
    }
    Assert.assertEquals(COUNT, count);
  }

  @Test public void testStrings() throws Exception {
    FILE.delete();

    ColumnFileWriter out =
      new ColumnFileWriter(new ColumnMetaData("test", ValueType.STRING));
    Random random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      out.writeRow(TestUtil.randomString(random));
    out.writeTo(FILE);

    random = TestUtil.createRandom();
    ColumnFileReader in = new ColumnFileReader(FILE);
    Assert.assertEquals(COUNT, in.getRowCount());
    Assert.assertEquals(1, in.getColumnCount());
    Iterator<String> i = in.getValues(0);
    int count = 0;
    while (i.hasNext()) {
      Assert.assertEquals(TestUtil.randomString(random), i.next());
      count++;
    }
    Assert.assertEquals(COUNT, count);
  }

  @Test public void testTwoColumn() throws Exception {
    FILE.delete();
    ColumnFileWriter out =
      new ColumnFileWriter(new ColumnMetaData("a", ValueType.FIXED32),
                           new ColumnMetaData("b", ValueType.STRING));
    Random random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      out.writeRow(random.nextInt(), TestUtil.randomString(random));
    out.writeTo(FILE);

    random = TestUtil.createRandom();
    ColumnFileReader in = new ColumnFileReader(FILE);
    Assert.assertEquals(COUNT, in.getRowCount());
    Assert.assertEquals(2, in.getColumnCount());
    Iterator<String> i = in.getValues(0);
    Iterator<String> j = in.getValues(1);
    int count = 0;
    while (i.hasNext() && j.hasNext()) {
      Assert.assertEquals(random.nextInt(), i.next());
      Assert.assertEquals(TestUtil.randomString(random), j.next());
      count++;
    }
    Assert.assertEquals(COUNT, count);
  }

}
