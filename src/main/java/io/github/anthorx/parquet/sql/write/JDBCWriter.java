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

import io.github.anthorx.parquet.sql.read.PreparedStatementRecordConsumer;
import io.github.anthorx.parquet.sql.read.RecordConsumerInitializer;
import io.github.anthorx.parquet.sql.read.SQLParquetReaderWrapper;
import io.github.anthorx.parquet.sql.record.ReadRecordConsumer;
import io.github.anthorx.parquet.sql.record.Record;
import io.github.anthorx.parquet.sql.record.RecordField;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

public class JDBCWriter {

  final private SQLParquetReaderWrapper parquetReaderWrapper;
  final private RecordConsumerInitializer lazyRecordConsumerInitializer;
  private int batchSize;


  public JDBCWriter(RecordConsumerInitializer lazyRecordConsumerInitializer, String filePath, int batchSize) throws IOException {
    this.lazyRecordConsumerInitializer = lazyRecordConsumerInitializer;
    this.parquetReaderWrapper = new SQLParquetReaderWrapper(filePath, new Configuration());
    this.batchSize = batchSize;
  }

  public JDBCWriter(RecordConsumerInitializer lazyRecordConsumerInitializer, SQLParquetReaderWrapper sqlParquetReaderWrapper, int batchSize) {
    this.lazyRecordConsumerInitializer = lazyRecordConsumerInitializer;
    this.parquetReaderWrapper = sqlParquetReaderWrapper;
    this.batchSize = batchSize;
  }


  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public void write() throws IOException, SQLException {
    int nbRecordInBatch = 0;
    List<String> fieldsNames = parquetReaderWrapper.getFieldsNames();
    List<Type> fields = parquetReaderWrapper.getFields();
    Record record = parquetReaderWrapper.read();

    if (record != null) {
      try (PreparedStatementRecordConsumer recordConsumer = lazyRecordConsumerInitializer.initialize(fieldsNames)) {
        do {
          List<RecordField> fieldsValues = extractFieldValues(fields, record);
          fieldsValues.forEach(field -> field.applyReadConsumer(recordConsumer));

          recordConsumer.addBatch();
          ++nbRecordInBatch;

          if (nbRecordInBatch >= batchSize) {
            recordConsumer.executeBatch();
            nbRecordInBatch = 0;
          }
        } while ((record = parquetReaderWrapper.read()) != null);

        if (nbRecordInBatch != 0) {
          recordConsumer.executeBatch();
        }
      } catch (SQLException e) {
        throw new SQLException("Error when writing Parquet records to the target table.", e);
      }
    }
  }

  public Future write(ExecutorService executorService) {
    if (executorService == null) {
      throw new IllegalArgumentException("The ExecutorService can't be null");
    }
    List<String> fieldsNames = parquetReaderWrapper.getFieldsNames();
    List<Type> fields = parquetReaderWrapper.getFields();

    return executorService.submit(() -> parquetReaderWrapper
        .toParallelStream()
        .forEach(currentReader -> {
          writerUntilReaderFinished(fieldsNames, fields, currentReader);
        }));
  }

  private void writerUntilReaderFinished(List<String> fieldsNames, List<Type> fields, ParquetReader<Record> reader) {
    Record record = getNextRecord(reader);
    int nbRecordInBatch = 0;

    System.out.println(String.format("Trehad for reader %s is %s", reader.toString(), Thread.currentThread().getId()));

    if (record != null) {
      // TODO: Rework connection in lazyRecordConsumerInitializer
      try (PreparedStatementRecordConsumer recordConsumer = lazyRecordConsumerInitializer.initialize(fieldsNames)) {
        do {
          List<RecordField> fieldsValues = extractFieldValues(fields, record);
          fieldsValues.forEach(field -> field.applyReadConsumer(recordConsumer));

          recordConsumer.addBatch();
          ++nbRecordInBatch;

          if (nbRecordInBatch >= batchSize) {
            recordConsumer.executeBatch();
            nbRecordInBatch = 0;
          }
        } while ((record = getNextRecord(reader)) != null);

        if (nbRecordInBatch != 0) {
          recordConsumer.executeBatch();
        }
      } catch (SQLException e) {
        throw new JDBCWriterException("Error when writing Parquet records to the target table.", e);
      }
    }
  }

  private Record getNextRecord(ParquetReader<Record> reader) {
    try {
      return reader.read();
    } catch (IOException e) {
      throw new JDBCWriterException("Can't read the next record: " + e.getMessage(), e);
    }
  }

  private List<RecordField> extractFieldValues(List<Type> fields, Record record) {
    return fields
        .stream()
        .reduce(new ArrayList<>(), (currentValues, currentField) -> {
          RecordField result = record
              .getField(currentField.getName())
              .orElse(createNullRecordField(currentField));
          currentValues.add(result);
          return currentValues;
        }, (before, after) -> after);
  }

  private RecordField createNullRecordField(Type currentField) {
    RecordField<Object> recordField = new RecordField<>(currentField.getName(), null);
    BiConsumer<ReadRecordConsumer, Object> objectRecordConsumer = ReadRecordConsumer::setObject;
    recordField.addReadConsumer(objectRecordConsumer);

    return recordField;
  }
}
