package io.github.anthorx.parquet.sql.converter;

import io.github.anthorx.parquet.sql.converter.types.ParquetSQLConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ConverterContainer {

    private List<ParquetSQLConverter> container;

    public ConverterContainer() {
        container = new ArrayList<>();
    }

    public void registerConverter(ParquetSQLConverter converter) {
        container.add(converter);
    }

    public Optional<ParquetSQLConverter> getFirstConverter(String classFullName) throws ClassNotFoundException {
        return getFirstConverter(Class.forName(classFullName));
    }

    public Optional<ParquetSQLConverter> getFirstConverter(Class<?> clazz) {
        return container
                .stream()
                .filter(converter -> converter.accept(clazz))
                .findFirst();
    }
}
