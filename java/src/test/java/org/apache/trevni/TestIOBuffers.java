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

import java.util.Random;

import java.io.ByteArrayOutputStream;

import org.junit.Assert;
import org.junit.Test;

public class TestIOBuffers {

  private static final int COUNT = 1000;

  @Test public void testEmpty() throws Exception {
    OutputBuffer out = new OutputBuffer();
    ByteArrayOutputStream temp = new ByteArrayOutputStream();
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    Assert.assertEquals(0, in.tell());
    Assert.assertEquals(0, in.length());
  }

  @Test public void testZero() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    out.writeInt(0);
    byte[] bytes = out.toByteArray();
    Assert.assertEquals(1, bytes.length);
    Assert.assertEquals(0, bytes[0]);
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    Assert.assertEquals(0, in.readInt());
  }

  @Test public void testInt() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeInt(random.nextInt());
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(random.nextInt(), in.readInt());
  }

  @Test public void testLong() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeLong(random.nextLong());
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(random.nextLong(), in.readLong());
  }

  @Test public void testFixed32() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeFixed32(random.nextInt());
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(random.nextInt(), in.readFixed32());
  }

  @Test public void testFixed64() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeFixed64(random.nextLong());
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(random.nextLong(), in.readFixed64());
  }

  @Test public void testBytes() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeBytes(TestUtil.randomBytes(random));
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertArrayEquals(TestUtil.randomBytes(random), in.readBytes());
  }

  @Test public void testString() throws Exception {
    Random random = TestUtil.createRandom();
    OutputBuffer out = new OutputBuffer();
    for (int i = 0; i < COUNT; i++)
      out.writeString(TestUtil.randomString(random));
    
    InputBuffer in = new InputBuffer(new InputBytes(out.toByteArray()));
    random = TestUtil.createRandom();
    for (int i = 0; i < COUNT; i++)
      Assert.assertEquals(TestUtil.randomString(random), in.readString());
  }

}
