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
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class SQLParquetReaderWrapper {

  private final ParquetReader<Record> parquetReader;
  private final MessageType schema;
  private final SQLGroupConverter groupConverter;

  public SQLParquetReaderWrapper(String filePath) throws IOException {
    InputFile inputFile = HadoopInputFile.fromPath(new Path(filePath), new Configuration());

    ParquetReader.Builder<Record> recordParquetReader = new SQLParquetReaderWrapper.Builder(inputFile);

    this.parquetReader = recordParquetReader.build();
    this.schema = this.initFileSchema(inputFile);
    this.groupConverter = new SQLGroupConverter(this.schema);
  }

  private MessageType initFileSchema(InputFile inputFile) throws IOException {
    ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile);
    MessageType schema = parquetFileReader.getFileMetaData().getSchema();
    parquetFileReader.close();

    return schema;
  }

  public MessageType getSchema() {
    return schema;
  }

  public Record read() throws IOException {
    return parquetReader.read();
  }

  public List<String> getFieldsNames() {
    return this.schema
        .getFields()
        .stream()
        .map(Type::getName)
        .collect(Collectors.toList());
  }

  public List<Type> getFields() {
    return this.schema
        .getFields();
  }

  public Converter getConverterFromField(Type field) {
    return groupConverter.getConverterFromField(field);
  }

  public static class Builder extends ParquetReader.Builder<Record> {

    protected Builder(InputFile file) {
      super(file);
    }

    protected ReadSupport<Record> getReadSupport() {
      return new SQLReadSupport();
    }

    @Override
    public ParquetReader<Record> build() throws IOException {
      return super.build();
    }
  }
}
