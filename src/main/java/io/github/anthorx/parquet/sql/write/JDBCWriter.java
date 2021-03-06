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
import io.github.anthorx.parquet.sql.record.Record;
import io.github.anthorx.parquet.sql.record.RecordField;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

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
    List<Type> schemaFields = parquetReaderWrapper.getFields();
    Record record = parquetReaderWrapper.read();

    if (record != null) {
      try (PreparedStatementRecordConsumer recordConsumer = lazyRecordConsumerInitializer.initialize(fieldsNames)) {
        do {
          List<RecordField<?>> fieldsValues = extractRecordField(schemaFields, record);
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

  /**
   * based on schemaField,
   * extract recordField from record or create nullRecordField
   *
   * @param schemaField the required fields
   * @param record      the record
   * @return the full recordFields
   */
  private List<RecordField<?>> extractRecordField(List<Type> schemaField, Record record) {
    return schemaField.stream().map(type -> record
        .getField(type.getName())
        .orElseGet(() -> NullRecordFieldConstructor.newNullRecordField(type)))
        .collect(Collectors.toCollection(Vector::new));
  }
}
