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

import org.junit.Assert;
import org.junit.Test;

public class TestColumnFile {

  private static final File FILE = new File("target", "test.trv");
  private static final int SIZE = 1000;
  private static final int COUNT = 100;

  @Test public void testEmpty() throws Exception {
    FILE.delete();
    ColumnFileWriter out = new ColumnFileWriter();
    out.writeTo(FILE);
    ColumnFileReader in = new ColumnFileReader(FILE);
    Assert.assertEquals(0, in.getRowCount());
    Assert.assertEquals(0, in.getColumnCount());
    in.close();
  }

  @Test public void testInt() throws Exception {
    FILE.delete();

    ColumnFileWriter out =
      new ColumnFileWriter(new ColumnMetaData("test", ValueType.INT));
    Random random = new Random();
    out.writeRow(random.nextInt(SIZE));
    out.writeTo(FILE);

    ColumnFileReader in = new ColumnFileReader(FILE);
    Assert.assertEquals(1, in.getRowCount());
    Assert.assertEquals(1, in.getColumnCount());
  }
}
