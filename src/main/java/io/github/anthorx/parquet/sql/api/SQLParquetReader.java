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

package io.github.anthorx.parquet.sql.api;

import io.github.anthorx.parquet.sql.parquet.read.SQLReadSupport;
import io.github.anthorx.parquet.sql.parquet.model.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class SQLParquetReader {

  private final Iterator<ParquetReader<Record>> parquetReaderIterator;
  private final Configuration configuration;
  private ParquetReader<Record> currentParquetReader;
  private MessageType schema;

  public SQLParquetReader(String stringFilePath, Configuration configuration) throws IOException {
    this.configuration = configuration;
    Path filePath = new Path(stringFilePath);
    FileSystem fileSystem = filePath.getFileSystem(configuration);
    FileStatus fileStatus = fileSystem.getFileStatus(filePath);

    if(fileStatus.isDir()) {
      this.parquetReaderIterator = createReadersFromFolderAndInitSchema(fileSystem, filePath, configuration);
    } else {
      this.parquetReaderIterator = createReadersFromFileAndInitSchema(fileStatus, configuration);
    }
  }

  private Iterator<ParquetReader<Record>> createReadersFromFolderAndInitSchema(FileSystem fileSystem, Path filePath,
                                                                               Configuration configuration) throws IOException {
    List<ParquetReader<Record>> parquetReaderList = new ArrayList<>();
    for (FileStatus currentFileStatus : fileSystem.listStatus(filePath, HiddenFileFilter.INSTANCE)) {
      InputFile currentInputFile = HadoopInputFile.fromStatus(currentFileStatus, configuration);

      if (schema == null) {
        this.initFileSchema(currentInputFile);
      }

      ParquetReader.Builder<Record> builder = new SQLParquetReader.Builder(currentInputFile);
      parquetReaderList.add(builder.build());
    }

    return parquetReaderList.iterator();
  }

  private Iterator<ParquetReader<Record>> createReadersFromFileAndInitSchema(FileStatus fileStatus, Configuration configuration) throws IOException {
    InputFile inputFile = HadoopInputFile.fromStatus(fileStatus, configuration);
    ParquetReader.Builder<Record> recordParquetReader = new SQLParquetReader.Builder(inputFile);

    this.initFileSchema(inputFile);
    return Collections.singletonList(recordParquetReader.build()).iterator();
  }

  private void initFileSchema(InputFile inputFile) throws IOException {
    ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile);
    MessageType schema = parquetFileReader.getFileMetaData().getSchema();
    parquetFileReader.close();

    this.schema = schema;
  }

  public MessageType getSchema() {
    return schema;
  }

  public Record read() throws IOException {
    if (currentParquetReader == null) {
      return readFromNextParquetReader();
    }

    Record result = currentParquetReader.read();

    if (result == null) {
      return readFromNextParquetReader();
    }

    return result;
  }

  private Record readFromNextParquetReader() throws IOException {
    if (parquetReaderIterator.hasNext()) {
      currentParquetReader = parquetReaderIterator.next();
      return currentParquetReader.read();
    } else {
      return null;
    }
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
