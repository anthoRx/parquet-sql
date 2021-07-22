package io.github.anthorx.parquet.sql.api;

import io.github.anthorx.parquet.sql.jdbc.model.SQLField;
import io.github.anthorx.parquet.sql.jdbc.model.SQLRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JDBCReaderTest {

  @Mock
  private DataSource dataSource;

  @Mock
  private Connection connection;

  @Mock
  private PreparedStatement preparedStatement;

  @Mock
  private ResultSet resultSet;

  @Mock
  private ResultSetMetaData resultSetMetaData;

  private final int fetchSize = 1;
  private final String tableName = "tableName";
  private final String columnName = "columnName";
  private final int columnType = Types.INTEGER;
  private final int precision= 0;
  private final int scale = 0;
  private final String className= "java.lang.Integer";
  private final Integer value = 1;

  private JDBCReader jdbcReader;

  @BeforeEach
  public void setup() throws SQLException {
    int fetchSize = 1;

    doReturn(connection).when(dataSource).getConnection();
    doReturn(preparedStatement).when(connection).prepareStatement(anyString());
    doReturn(resultSet).when(preparedStatement).executeQuery();
    doReturn(resultSetMetaData).when(resultSet).getMetaData();
    doReturn(1).when(resultSetMetaData).getColumnCount();
    doReturn(columnName).when(resultSetMetaData).getColumnName(1);
    doReturn(columnType).when(resultSetMetaData).getColumnType(1);
    doReturn(precision).when(resultSetMetaData).getPrecision(1);
    doReturn(scale).when(resultSetMetaData).getScale(1);
    doReturn(className).when(resultSetMetaData).getColumnClassName(1);

    jdbcReader = new JDBCReader(dataSource, "tableName", fetchSize);
  }

  @Test
  void initialization_ok() throws SQLException {
    verify(dataSource).getConnection();
    verify(connection).prepareStatement("select * from tableName");
    verify(preparedStatement).executeQuery();
    verify(preparedStatement).setFetchSize(fetchSize);
    verify(resultSet).getMetaData();
    verify(resultSetMetaData).getColumnCount();
    verify(resultSetMetaData).getColumnName(1);
    verify(resultSetMetaData).getColumnType(1);
    verify(resultSetMetaData).getPrecision(1);
    verify(resultSetMetaData).getScale(1);
    verify(resultSetMetaData).getColumnClassName(1);
  }
  @Test
  void getMetaData() throws SQLException {
    // resultSet has already been called in jdbcReader constructor
    reset(resultSet);

    jdbcReader.getMetaData();

    verify(resultSet).getMetaData();
  }

  @Test
  void read() throws SQLException {
    // Given a row with a value
    doReturn(true).when(resultSet).next();
    doReturn(value).when(resultSet).getObject(1);

    SQLRow actual = jdbcReader.read();

    assertEquals(1, actual.getFields().size());

    SQLField field = actual.getField(0);
    assertEquals(columnName, field.getName());
    assertEquals(columnType, field.getSqlType());
    assertEquals(Optional.of(precision), field.getPrecision());
    assertEquals(Optional.of(scale), field.getScale());
    assertEquals(className, field.getColumnClassName());
    assertEquals(value, field.getValue());
  }

  @Test
  void read_no_row() throws SQLException {
    // Given no row
    doReturn(false).when(resultSet).next();

    SQLRow actual = jdbcReader.read();

    assertNull(actual);
  }

  @Test
  void close() throws SQLException {
    doReturn(connection).when(preparedStatement).getConnection();

    jdbcReader.close();

    verify(connection).close();
    verify(preparedStatement).close();
  }
}