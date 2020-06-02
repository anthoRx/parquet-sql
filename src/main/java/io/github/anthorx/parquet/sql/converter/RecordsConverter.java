package io.github.anthorx.parquet.sql.converter;

import io.github.anthorx.parquet.sql.converter.types.ParquetSQLConverter;
import io.github.anthorx.parquet.sql.model.Row;
import io.github.anthorx.parquet.sql.model.SQLField;
import io.github.anthorx.parquet.sql.record.RecordField;
import io.github.anthorx.parquet.sql.record.Records;

import java.util.Optional;

public class RecordsConverter implements Converter<Row, Records> {

    private ConverterContainer converterContainer;

    public RecordsConverter(ConverterContainer converterContainer) {
        this.converterContainer = converterContainer;
    }

    @Override
    public Records convert(Row row) throws ConvertException {
        Records records = new Records();

        for (SQLField sqlField : row.getFields()) {
            try {
                Optional<ParquetSQLConverter> converter = converterContainer.getFirstConverter(sqlField.getColumnClassName());
                RecordField<?> recordField = converter.orElseThrow(() -> new ConvertException("")).convert(sqlField);
                records.addField(recordField);
            } catch (ClassNotFoundException e) {
                throw new ConvertException("", e);
            }
        }

        return records;
    }

}
