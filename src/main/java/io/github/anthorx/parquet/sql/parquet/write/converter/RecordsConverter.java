package io.github.anthorx.parquet.sql.parquet.write.converter;

import io.github.anthorx.parquet.sql.parquet.model.RecordField;
import io.github.anthorx.parquet.sql.jdbc.model.SQLField;
import io.github.anthorx.parquet.sql.jdbc.model.SQLRow;
import io.github.anthorx.parquet.sql.parquet.model.Record;
import org.apache.parquet.io.api.RecordConsumer;

import java.util.function.BiConsumer;

public class RecordsConverter implements Converter<SQLRow, Record> {

  private ConverterContainer converterContainer;

  private final BiConsumer<RecordConsumer, Object> NO_OP_CONSUMER = (a, b) -> {};

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
              .addWriteConsumer(NO_OP_CONSUMER);
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
