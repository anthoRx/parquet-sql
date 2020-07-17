package io.github.anthorx.parquet.sql.converter;

import io.github.anthorx.parquet.sql.model.Row;
import io.github.anthorx.parquet.sql.model.SQLField;
import io.github.anthorx.parquet.sql.record.RecordField;
import io.github.anthorx.parquet.sql.record.Records;

public class RecordsConverter implements Converter<Row, Records> {

    private ConverterContainer converterContainer;

    public RecordsConverter(ConverterContainer converterContainer) {
        this.converterContainer = converterContainer;
    }

    @Override
    public Records convert(Row row) throws ConvertException {
        Records records = new Records();

        for (SQLField sqlField : row.getFields()) {
            String columnClassName = sqlField.getColumnClassName();
            try {
                RecordField<?> recordField;
                if (sqlField.getValue() == null) {
                    recordField = new RecordField<>(sqlField.getName(), null, (a, b) -> {
                    });
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
