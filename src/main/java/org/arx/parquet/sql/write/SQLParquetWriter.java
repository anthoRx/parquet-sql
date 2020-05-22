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

package org.arx.parquet.sql.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.arx.parquet.sql.converter.ConvertException;
import org.arx.parquet.sql.converter.MessageTypeConverter;
import org.arx.parquet.sql.model.Row;

import java.io.IOException;
import java.sql.ResultSetMetaData;

public class SQLParquetWriter extends ParquetWriter<Row> {

  /**
   * Constructor for retrocompatibility only. Use the builder rather than this constructor.
   *
   * @param file
   * @param writeSupport
   * @param compressionCodecName
   * @param blockSize
   * @param pageSize
   * @param enableDictionary
   * @param enableValidation
   * @param writerVersion
   * @param conf
   * @throws IOException
   */
  @Deprecated
  SQLParquetWriter(Path file, WriteSupport<Row> writeSupport, CompressionCodecName compressionCodecName, int blockSize, int pageSize, boolean enableDictionary, boolean enableValidation, ParquetProperties.WriterVersion writerVersion, Configuration conf) throws IOException {
    super(file, writeSupport, compressionCodecName, blockSize, pageSize, pageSize, enableDictionary, enableValidation, writerVersion, conf);
  }


  public static SQLParquetWriter.Builder builder(String file) {
    return new SQLParquetWriter.Builder(new Path(file));
  }

  private static WriteSupport<Row> writeSupport(Configuration conf, MessageType messageType) {
    return new SQLWriteSupport(messageType);
  }

  /**
   * Builder
   */
  public static class Builder extends org.apache.parquet.hadoop.ParquetWriter.Builder<Row, SQLParquetWriter.Builder> {

    private ResultSetMetaData resultSetMetaData;
    private String schemaName;
    MessageType messageType;

    private Builder(Path file) {
      super(file);
    }

    public SQLParquetWriter.Builder withSchema(String schemaName, ResultSetMetaData resultSetMetaData) {
      this.schemaName = schemaName;
      this.resultSetMetaData = resultSetMetaData;
      return this;
    }

    protected SQLParquetWriter.Builder self() {
      return this;
    }

    protected WriteSupport<Row> getWriteSupport(Configuration conf) {
      return SQLParquetWriter.writeSupport(conf, this.messageType);
    }

    public ParquetWriter<Row> build() throws IOException {
      try {
        this.messageType = new MessageTypeConverter(schemaName).convert(resultSetMetaData);
      } catch (ConvertException e) {
        throw new IOException("Error when building ParquetWriter. ResultSet structure can't be converted to a parquet schema", e);
      }
      return super.build();
    }
  }

}
