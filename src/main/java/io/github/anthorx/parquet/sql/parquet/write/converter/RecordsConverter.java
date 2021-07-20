package io.github.anthorx.parquet.sql.parquet.write.converter;

import io.github.anthorx.parquet.sql.parquet.model.ParquetRecordField;
import io.github.anthorx.parquet.sql.jdbc.model.SQLField;
import io.github.anthorx.parquet.sql.jdbc.model.SQLRow;
import io.github.anthorx.parquet.sql.parquet.model.ParquetRecord;
import org.apache.parquet.io.api.RecordConsumer;

import java.util.function.BiConsumer;

public class RecordsConverter implements Converter<SQLRow, ParquetRecord> {

  private ConverterContainer converterContainer;

  private final BiConsumer<RecordConsumer, Object> NO_OP_CONSUMER = (a, b) -> {};

  public RecordsConverter(ConverterContainer converterContainer) {
    this.converterContainer = converterContainer;
  }

  @Override
  public ParquetRecord convert(SQLRow row) throws ConvertException {
    ParquetRecord records = new ParquetRecord();

    for (SQLField sqlField : row.getFields()) {
      String columnClassName = sqlField.getColumnClassName();
      try {
        ParquetRecordField<?> parquetRecordField;
        if (sqlField.getValue() == null) {
          parquetRecordField = new ParquetRecordField<>(sqlField.getName(), null)
              .addWriteConsumer(NO_OP_CONSUMER);
        } else {
          parquetRecordField = converterContainer
              .getConverter(columnClassName)
              .orElseThrow(() -> new ConvertException("No converter found for class " + columnClassName))
              .convert(sqlField);
        }
        records.addField(parquetRecordField);
      } catch (ClassNotFoundException e) {
        throw new ConvertException("Impossible to convert " + sqlField + ". Class not found", e);
      }
    }

    return records;
  }
}
