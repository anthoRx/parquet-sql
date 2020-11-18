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

package io.github.anthorx.parquet.sql.write;

import io.github.anthorx.parquet.sql.model.SQLRow;
import io.github.anthorx.parquet.sql.write.converter.ConvertException;
import io.github.anthorx.parquet.sql.write.converter.ConverterContainer;
import io.github.anthorx.parquet.sql.write.converter.MessageTypeConverter;
import io.github.anthorx.parquet.sql.write.converter.types.ParquetSQLConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.sql.ResultSetMetaData;

public class SQLParquetWriter extends ParquetWriter<SQLRow> {


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
  SQLParquetWriter(Path file, WriteSupport<SQLRow> writeSupport, CompressionCodecName compressionCodecName, int blockSize, int pageSize, boolean enableDictionary, boolean enableValidation, ParquetProperties.WriterVersion writerVersion, Configuration conf) throws IOException {
    super(file, writeSupport, compressionCodecName, blockSize, pageSize, pageSize, enableDictionary, enableValidation, writerVersion, conf);
  }


  public static SQLParquetWriter.Builder builder(String file) {
    return new SQLParquetWriter.Builder(new Path(file));
  }

  /**
   * Builder
   */
  public static class Builder extends ParquetWriter.Builder<SQLRow, SQLParquetWriter.Builder> {

    private ResultSetMetaData resultSetMetaData;
    private String schemaName;
    private ConverterContainer converterContainer;
    MessageType messageType;

    private Builder(Path file) {
      super(file);
    }

    public SQLParquetWriter.Builder withSchema(String schemaName, ResultSetMetaData resultSetMetaData) {
      this.schemaName = schemaName;
      this.resultSetMetaData = resultSetMetaData;
      this.converterContainer = new ConverterContainer();
      return this;
    }

    public SQLParquetWriter.Builder registerConverter(ParquetSQLConverter parquetSQLConverter) {
      converterContainer.registerConverter(parquetSQLConverter);
      return this;
    }

    protected SQLParquetWriter.Builder self() {
      return this;
    }

    protected WriteSupport<SQLRow> getWriteSupport(Configuration conf) {
      return new SQLWriteSupport(messageType, converterContainer);
    }

    public ParquetWriter<SQLRow> build() throws IOException {
      try {
        this.messageType = new MessageTypeConverter(schemaName, converterContainer).convert(resultSetMetaData);
      } catch (ConvertException e) {
        throw new IOException("Error when building ParquetWriter. ResultSet structure can't be converted to a parquet schema", e);
      }
      return super.build();
    }
  }

}
