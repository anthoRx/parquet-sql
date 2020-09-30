# parquet-sql


## Maven dependency
```xml
<dependency>
    <groupId>io.github.anthorx</groupId>
    <artifactId>parquet-sql</artifactId>
    <version>0.1-SNAPSHOT</version>
</dependency>
```

## How to serialize a ResultSet in a Parquet file 
```java
    ResultSet resultSet = /* statement */;
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    
    ParquetWriter<SQLRow> parquetWriter = SQLParquetWriter
            .builder("FileName")
            .withSchema("schemaName", resultSetMetaData)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build();
    
    while (resultSet.next()) {
        SQLRow row = SQLRowFactory.createSQLRow(rs);
        parquetWriter.write(row);
    }

    parquetWriter.close();
```

# How to read a Parquet file to insert in a SQL database
```java
    ParquetReader<Record> parquetReader = SQLParquetReader
        .builder(filePath)
        .build();

    PreparedStatement preparedStatement = con.prepareStatement(insertQuery);
    PreparedStatementRecordConsumer psrc = new PreparedStatementRecordConsumer(preparedStatement);

    Record record;
    while ((record = parquetReader.read()) != null) {
      record
          .getFields()
          .forEach( f -> f.applyReadConsumer(psrc));
      psrc.addBatch();
    }

    preparedStatement.executeBatch();
```