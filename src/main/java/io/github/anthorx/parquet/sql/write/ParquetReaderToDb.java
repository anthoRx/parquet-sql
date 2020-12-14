package io.github.anthorx.parquet.sql.write;

import io.github.anthorx.parquet.sql.read.PreparedStatementRecordConsumer;
import io.github.anthorx.parquet.sql.record.ReadRecordConsumer;
import io.github.anthorx.parquet.sql.record.Record;
import io.github.anthorx.parquet.sql.record.RecordField;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class ParquetReaderToDb {

  private final ParquetReader<Record> reader;
  private final PreparedStatementRecordConsumer recordConsumer;
  private final List<Type> fields;

  public ParquetReaderToDb(ParquetReader<Record> reader, PreparedStatementRecordConsumer recordConsumer, List<Type> fields) {
    this.reader = reader;
    this.recordConsumer = recordConsumer;
    this.fields = fields;
  }

  public void compute(int batchSize) {
    Record record = getNextRecord(reader);
    int nbRecordInBatch = 0;

    if (record != null) {
      try {
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
