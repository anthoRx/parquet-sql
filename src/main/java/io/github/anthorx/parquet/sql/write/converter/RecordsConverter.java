package io.github.anthorx.parquet.sql.write.converter;

import io.github.anthorx.parquet.sql.model.SQLField;
import io.github.anthorx.parquet.sql.model.SQLRow;
import io.github.anthorx.parquet.sql.record.RecordField;
import io.github.anthorx.parquet.sql.record.Record;

public class RecordsConverter implements Converter<SQLRow, Record> {

  private ConverterContainer converterContainer;

  public RecordsConverter(ConverterContainer converterContainer) {
    this.converterContainer = converterContainer;
  }

  @Override
  public Record convert(SQLRow row) throws ConvertException {
    Record records = new Record();

    for (SQLField sqlField : row.getFields()) {
      String columnClassName = sqlField.getColumnClassName();
      try {
        RecordField<?> recordField;
        if (sqlField.getValue() == null) {
          recordField = new RecordField<>(sqlField.getName(), null)
              .addWriteConsumer((a, b) -> {});
        } else {
          recordField = converterContainer
              .getConverter(columnClassName)
              .orElseThrow(() -> new ConvertException("No converter found for class " + columnClassName))
              .convert(sqlField);
        }
        records.addField(recordField);
      } catch (ClassNotFoundException e) {
        throw new ConvertException("Impossible to convert " + sqlField + ". Class not found", e);
      }
    }

    return records;
  }
}
