package io.github.anthorx.parquet.sql.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class JDBCWriterTest {

  @Mock
  private Connection connection;

  @Mock
  private PreparedStatement preparedStatement;

  private JDBCWriter jdbcWriter;

  @BeforeEach
  public void jdbcWriter() throws SQLException {
    doReturn(preparedStatement).when(connection).prepareStatement(anyString());
    jdbcWriter = new JDBCWriter(connection, "tableName", Collections.singleton("column"));
  }

  @Test
  public void prepareStatementQuery() {
    String result = JDBCWriter.prepareStatementQuery("car", Arrays.asList("brand", "color", "price"));

    String expected = "insert into car(brand,color,price) values (?,?,?)";

    Assertions.assertEquals(expected, result);
  }

  @Test
  public void JDBCWriter_whenNoField_throwsException() {
    assertThrows(IllegalArgumentException.class, () -> {
      new JDBCWriter(connection, "car", Collections.emptyList());
    });
  }

  @Test
  public void addBatch_ok() throws SQLException {
    jdbcWriter.addBatch();

    verify(preparedStatement, times(1)).addBatch();
    verify(preparedStatement, times(1)).clearParameters();
  }

  @Test
  public void addBatch_setTest_errors() throws SQLException {

    // Given an error in setBoolean
    doThrow(new SQLException("mock exception")).when(preparedStatement).setBoolean(anyInt(), anyBoolean());
    jdbcWriter.setBoolean(true);

    // Then exception is thrown when addBatch is called
    assertThrows(SQLException.class, () -> jdbcWriter.addBatch());
  }

  @Test
  public void executeBatch() throws SQLException {
    jdbcWriter.executeBatch();

    verify(preparedStatement, times(1)).executeLargeBatch();
  }

  @Test
  public void setTest() throws SQLException {
    jdbcWriter.setBoolean(true);
    jdbcWriter.setByte((byte) 1);
    jdbcWriter.setShort((short) 10);
    jdbcWriter.setInt(10);
    jdbcWriter.setLong(10);
    jdbcWriter.setFloat(10);
    jdbcWriter.setDouble(10);
    jdbcWriter.setBigDecimal(new BigDecimal(10));
    jdbcWriter.setString("10");
    jdbcWriter.setBytes("10".getBytes());
    jdbcWriter.setDate(Date.valueOf("2020-01-01"));
    jdbcWriter.setTimestamp(Timestamp.valueOf("2020-01-01 00:00:00"));
    jdbcWriter.setObject(10);
    jdbcWriter.setNull(java.sql.Types.INTEGER);

    verify(preparedStatement, times(1)).setBoolean(1, true);
    verify(preparedStatement, times(1)).setByte(2, (byte) 1);
    verify(preparedStatement, times(1)).setShort(3, (short) 10);
    verify(preparedStatement, times(1)).setInt(4, 10);
    verify(preparedStatement, times(1)).setLong(5, 10);
    verify(preparedStatement, times(1)).setFloat(6, 10);
    verify(preparedStatement, times(1)).setDouble(7, 10);
    verify(preparedStatement, times(1)).setBigDecimal(8, new BigDecimal(10));
    verify(preparedStatement, times(1)).setString(9, "10");
    verify(preparedStatement, times(1)).setBytes(10, "10".getBytes());
    verify(preparedStatement, times(1)).setDate(11, Date.valueOf("2020-01-01"));
    verify(preparedStatement, times(1)).setTimestamp(12, Timestamp.valueOf("2020-01-01 00:00:00"));
    verify(preparedStatement, times(1)).setObject(13, 10);
    verify(preparedStatement, times(1)).setNull(14, java.sql.Types.INTEGER);
  }

  @Test
  public void close() throws SQLException {
    doReturn(connection).when(preparedStatement).getConnection();
    jdbcWriter.close();

    verify(connection, times(1)).close();
    verify(preparedStatement, times(1)).close();
  }

  @Test
  public void JDBCWriter_whenTimezone_passToPrepareStatement() throws SQLException {
    // Given JDBCWriters instantiated with a Timezone
    TimeZone tz = TimeZone.getTimeZone("America/Los_Angeles");
    doReturn(preparedStatement).when(connection).prepareStatement(anyString());
    JDBCWriter jdbcWriter = new JDBCWriter(connection, "tableName", Collections.singleton("column"), tz);
    JDBCWriter jdbcWriterPs = new JDBCWriter(preparedStatement, tz);

    // When defining a date and/or a timestamp value
    jdbcWriter.setDate(Date.valueOf("2020-01-01"));
    jdbcWriter.setTimestamp(Timestamp.valueOf("2020-01-01 00:00:00"));
    jdbcWriterPs.setDate(Date.valueOf("2020-01-01"));
    jdbcWriterPs.setTimestamp(Timestamp.valueOf("2020-01-01 00:00:00"));

    // Then the timestamp value is pass to the PrepareStatement
    verify(preparedStatement, times(2))
        .setDate(
            eq(1),
            eq(Date.valueOf("2020-01-01")),
            argThat((c) -> c.getTimeZone().equals(tz))
        );

    verify(preparedStatement, times(2))
        .setTimestamp(
            eq(2),
            eq(Timestamp.valueOf("2020-01-01 00:00:00")),
            argThat((c) -> c.getTimeZone().equals(tz))
        );
  }
}
