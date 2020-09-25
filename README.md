# parquet-sql

## (Temporary) snapshot repository

Since this library is not yet released, you need to tell maven to fetch it from the snapshot repository

```xml
<profiles>
    <profile>
        <id>allow-snapshots</id>
        <activation><activeByDefault>true</activeByDefault></activation>
        <repositories>
            <repository>
                <id>snapshots-repo</id>
                <url>https://oss.sonatype.org/content/repositories/snapshots</url>
                <releases><enabled>false</enabled></releases>
                <snapshots><enabled>true</enabled></snapshots>
            </repository>
        </repositories>
    </profile>
</profiles>
```

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
