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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.avro.mapred.AvroWrapper;

/** An {@link org.apache.hadoop.mapred.InputFormat} for Trevni files */
public class AvroTrevniInputFormat<T>
  extends FileInputFormat<AvroWrapper<T>, NullWritable> {

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }

  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    job.setBoolean("mapred.input.dir.recursive", true);
    for (FileStatus file : super.listStatus(job))
      if (file.getPath().getName().endsWith(AvroTrevniOutputFormat.EXT))
        result.add(file);
    return result.toArray(new FileStatus[0]);
  }

  @Override
  public RecordReader<AvroWrapper<T>, NullWritable>
    getRecordReader(InputSplit split, final JobConf job,
                    Reporter reporter) throws IOException {
    final FileSplit file = (FileSplit)split;
    reporter.setStatus(file.toString());
    return new RecordReader<AvroWrapper<T>, NullWritable>() {
      private AvroColumnReader<T> reader =
        new AvroColumnReader<T>
        (new AvroColumnReader.Params(new HadoopInput(file.getPath(), job)));
      private float rows = reader.getRowCount();
      private long row;

      public AvroWrapper<T> createKey() { return new AvroWrapper<T>(null); }
  
      public NullWritable createValue() { return NullWritable.get(); }
    
      public boolean next(AvroWrapper<T> wrapper, NullWritable ignore)
        throws IOException {
        if (!reader.hasNext())
          return false;
        wrapper.datum(reader.next());
        row++;
        return true;
      }
  
      public float getProgress() throws IOException { return row / rows; }
  
      public long getPos() throws IOException { return row; }

      public void close() throws IOException { reader.close(); }
  
    };

  }

}

