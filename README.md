# parquet-sql

## Maven dependency
```xml
<dependency>
    <groupId>io.github.anthorx</groupId>
    <artifactId>parquet-sql</artifactId>
    <version>0.1-SNAPSHOT</version>
</dependency>
```

## Usage Example
```java
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import io.github.anthorx.parquet.sql.model.Row;
import io.github.anthorx.parquet.sql.model.RowFactory;
import io.github.anthorx.parquet.sql.write.SQLParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;

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
