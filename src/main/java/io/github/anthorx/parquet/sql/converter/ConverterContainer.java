package io.github.anthorx.parquet.sql.converter;

import io.github.anthorx.parquet.sql.converter.types.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ConverterContainer {

    private List<ParquetSQLConverter> container;

    public ConverterContainer() {
        container = new ArrayList<>();
        registerDefaultConverters();
    }

    private void registerDefaultConverters() {
        container.add(new BigDecimalConverter());
        container.add(new DoubleConverter());
        container.add(new StringConverter());
        container.add(new TimestampConverter());
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
