package io.github.anthorx.parquet.sql.converter;

import io.github.anthorx.parquet.sql.converter.types.*;

import java.util.*;

public class ConverterContainer {

    private Deque<ParquetSQLConverter> container;

    public ConverterContainer() {
        container = new LinkedList<>();
        registerDefaultConverters();
    }

    private void registerDefaultConverters() {
        container.add(new BigDecimalConverter());
        container.add(new DoubleConverter());
        container.add(new StringConverter());
        container.add(new TimestampConverter());
    }

    public void registerConverter(ParquetSQLConverter converter) {
        container.addFirst(converter);
    }

    /**
     * @return the last registered converter matching the given classFullName
     */
    public Optional<ParquetSQLConverter> getConverter(String classFullName) throws ClassNotFoundException {
        return getConverter(Class.forName(classFullName));
    }

    /**
     * @return the last registered converter matching the given clazz
     */
    public Optional<ParquetSQLConverter> getConverter(Class<?> clazz) {
        return container
                .stream()
                .filter(converter -> converter.accept(clazz))
                .findFirst();
    }
}
