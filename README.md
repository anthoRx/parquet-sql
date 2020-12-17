# parquet-sql


## Maven dependency
```xml
<dependency>
    <groupId>io.github.anthorx</groupId>
    <artifactId>parquet-sql</artifactId>
    <version>0.4</version>
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
To insert in a SQL database records of a Parquet file, you have to initialize a RecordConsumerInitializer and JDBCWriter.  

The RecordConsumerInitializer allows to provide information on  database connection and on the target table.  

JDBCWriter is responsible to write the records of the provided Parquet file to the Database, according to the RecordConsumerInitializer initialized before. The batchSize has to be provided too.
 

```java
    RecordConsumerInitializer consumerInitializer = new RecordConsumerInitializer(connection, "myTable");

    JDBCWriter jdbcWriter = new JDBCWriter(consumerInitializer, "/tmp/file.parquet", 50000);
    jdbcWriter.write();
```
