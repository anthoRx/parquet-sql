# parquet-sql


## Maven dependency

Pick last version in github release page:
https://github.com/anthoRx/parquet-sql/releases

```xml
<dependency>
    <groupId>io.github.anthorx</groupId>
    <artifactId>parquet-sql</artifactId>
    <version>${parquet-sql.version}</version>
</dependency>
```

## Read from SQL table to write into a Parquet
```java
    JDBCReader jdbcReader = new JDBCReader(dataSource, "tableName", fetchSize);
    
    ParquetWriter<SQLRow> parquetWriter = SQLParquetWriter
        .builder("fileName.parquet")
        .withSchema("schemaName", resultSetMetaData)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build();
    
    SQLRow currentRow = jdbcReader.read();
    
    while (currentRow != null) {
        parquetWriter.write(currentRow);
        currentRow = jdbcReader.read();
    }
    
    parquetWriter.close();
    jdbcReader.close();
```

# Read from Parquet to write into SQL table

```java
    SQLParquetReader parquetReader = new SQLParquetReader("fileName.parquet", new Configuration());
    
    JDBCWriter jdbcWriter = new JDBCWriter(dataSource, "tableName", parquetReader.getFieldsNames());
    
    Record currentRecord = parquetReader.read();
    while (currentRecord != null) {
        currentRecord.readAll(field -> field.read(jdbcWriter));
    
        jdbcWriter.addBatch();
        currentRecord = parquetReader.read();
    }
    jdbcWriter.executeBatch();

    parquetReader.close();
    jdbcWriter.close();
```
