# parquet-sql

## Example
```java
ResultSet resultSet = null; // To define
ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
ParquetWriter<Row> parquetWriter = SQLParquetWriter.builder("FileName")
        .withSchema("schemaName", resultSetMetaData)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build();

while (resultSet.next()) {
    Row row = RowFactory.createRow(resultSet);
    parquetWriter.write(row);
}
parquetWriter.close();
```
