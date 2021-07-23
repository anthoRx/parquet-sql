package io.github.anthorx.parquet.sql.api;

import io.github.anthorx.parquet.sql.jdbc.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.jdbc.model.SQLField;
import io.github.anthorx.parquet.sql.jdbc.model.SQLRow;
import io.github.anthorx.parquet.sql.parquet.model.RecordField;
import io.github.anthorx.parquet.sql.parquet.write.converter.ConvertException;
import io.github.anthorx.parquet.sql.parquet.write.converter.PrimitiveTypeCreator;
import io.github.anthorx.parquet.sql.parquet.write.converter.types.ParquetSQLConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.InvalidSchemaException;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
class SQLParquetWriterTest {

  private static final String schema = "schema";
  private final String parquetPath = getClass().getResource("").getPath() + "build.parquet";

  @Mock
  ResultSetMetaData resultSetMetaData;

  private SQLParquetWriter.Builder builder() {
    return SQLParquetWriter.builder(parquetPath);
  }

  @BeforeEach
  void setup() {
    File file = new File(parquetPath);
    if (file.exists() && ! file.delete()) {
      throw new RuntimeException(String.format("Could not delete file %s", parquetPath));
    }
  }

  @Test
  void builder_noColumns_throwsException() throws SQLException {
    // Given no columns
    doReturn(0).when(resultSetMetaData).getColumnCount();

    SQLParquetWriter.Builder builder = builder()
        .withSchema(schema, resultSetMetaData);

    assertThrows(InvalidSchemaException.class, builder::build);
  }


  @Test
  void builder_sqlException_throwsException() throws SQLException {
    doThrow(new SQLException("mocked exception")).when(resultSetMetaData).getColumnCount();

    SQLParquetWriter.Builder builder = builder()
        .withSchema(schema, resultSetMetaData);

    assertThrows(IOException.class, builder::build);
  }
  
  @Test
  void builder_oneColumn_ok() throws IOException, SQLException {
    // Given one column of type Boolean
    doReturn(1).when(resultSetMetaData).getColumnCount();
    doReturn("java.math.BigDecimal").when(resultSetMetaData).getColumnClassName(1);
    doReturn("bigDecimal").when(resultSetMetaData).getColumnName(1);

    SQLParquetWriter.Builder builder = builder()
        .withSchema(schema, resultSetMetaData);

    assertSame(builder, builder.self());
    assertAndClose(builder.build());
  }

  @Test
  void builder_customRegister_ok() throws IOException, SQLException {
    // Given a SQLParquetWriter column type
    doReturn(1).when(resultSetMetaData).getColumnCount();
    doReturn("io.github.anthorx.parquet.sql.api.SQLParquetWriter").when(resultSetMetaData).getColumnClassName(1);
    doReturn("dummyColumn").when(resultSetMetaData).getColumnName(1);

    // Given a SQLParquetWriter converter into Double
    SQLParquetWriter.Builder builder = builder()
        .withSchema(schema, resultSetMetaData)
        .registerConverter(new ParquetSQLConverter() {
          @Override
          public boolean accept(Class<?> c) {
            return c.isAssignableFrom(SQLParquetWriter.class);
          }

          @Override
          public RecordField<?> convert(SQLField sqlField) throws ConvertException {
            return null;
          }

          @Override
          public PrimitiveType convert(SQLColumnDefinition sqlColumnDefinition) throws ConvertException {
            return PrimitiveTypeCreator.create(sqlColumnDefinition.getName(),
                PrimitiveType.PrimitiveTypeName.DOUBLE,
                null,
                sqlColumnDefinition.isNullable());
          }
        });

    assertAndClose(builder.build());
  }

  private void assertAndClose(ParquetWriter<SQLRow> build) throws IOException {
    assertNotNull(build);
    build.close();
  }

}