/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.github.anthorx.parquet.sql.read;

import io.github.anthorx.parquet.sql.record.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;

import java.io.IOException;

public class SQLParquetReader extends ParquetReader<Record> {


  @Deprecated
  public SQLParquetReader(Configuration conf, Path file, ReadSupport<Record> readSupport) throws IOException {
    super(conf, file, readSupport);
  }

  public static SQLParquetReader.Builder builder(InputFile file) {
    return new SQLParquetReader.Builder(file);
  }

  public static class Builder extends ParquetReader.Builder<Record> {

    protected Builder(InputFile file) {
      super(file);
    }

    protected ReadSupport<Record> getReadSupport() {
      return new SQLReadSupport();
    }
  }
}
