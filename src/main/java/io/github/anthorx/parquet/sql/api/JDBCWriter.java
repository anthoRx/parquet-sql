/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.github.anthorx.parquet.sql.api;

import io.github.anthorx.parquet.sql.jdbc.ReadRecordConsumer;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static io.github.anthorx.parquet.sql.util.AssertionUtils.notEmpty;
import static io.github.anthorx.parquet.sql.util.AssertionUtils.notNull;

/**
 * use a prepareStatement to do batch executions on JDBC writer
 */
public class JDBCWriter implements ReadRecordConsumer, AutoCloseable {

  private interface SQLExceptionRunnable {
    void run() throws SQLException;
  }

  private final PreparedStatement preparedStatement;
  private final List<String> errors;
  private int currentParameterIndex = 0;
  private TimeZone timeZone;

  /**
   * Default constructor that should be used.
   * TimeZone is used to specify which timezone should be considered for Date and Timestamp types.
   * If you want a custom prepare statement, use the other constructor.
   */
  public JDBCWriter(Connection connection, String tableName, Collection<String> columnNames, TimeZone timeZone) throws SQLException, IllegalArgumentException {
    this(connection.prepareStatement(prepareStatementQuery(tableName, columnNames)), timeZone);
  }

  /**
   * Default constructor that should be used
   * If you want a custom prepare statement, use the other constructor
   */
  public JDBCWriter(Connection connection, String tableName, Collection<String> columnNames) throws SQLException, IllegalArgumentException {
    this(connection.prepareStatement(prepareStatementQuery(tableName, columnNames)));
  }

  /**
   * Constructor with a custom prepareStatement.
   * If you want a custom prepareStatement, use this constructor.
   */
  public JDBCWriter(PreparedStatement preparedStatement) {
    this.preparedStatement = preparedStatement;
    this.errors = new ArrayList<>();
  }

  /**
   * Constructor with a custom prepareStatement.
   * If you want a custom prepareStatement, use this constructor.
   */
  public JDBCWriter(PreparedStatement preparedStatement, TimeZone timeZone) {
    this.preparedStatement = preparedStatement;
    this.errors = new ArrayList<>();
    this.timeZone = timeZone;
  }

  protected static String prepareStatementQuery(String tableName, Collection<String> columnNames) throws IllegalArgumentException {
    notNull(tableName, "tableName cannot be null");
    notEmpty(columnNames, "columnNames cannot be empty");

    String formattedColumns = columnNames
        .stream()
        .collect(Collectors.joining(",", "(", ")"));

    String elem = String.join(",", Collections.nCopies(columnNames.size(), "?"));

    return String.format("insert into %s%s values (%s)", tableName, formattedColumns, elem);
  }

  public void addBatch() throws SQLException {
    if (!errors.isEmpty()) {
      throw new SQLException("Errors when setting prepared statement. Details : " + errors);
    } else {
      this.preparedStatement.addBatch();
      this.preparedStatement.clearParameters();
      this.currentParameterIndex = 0;
      this.errors.clear();
    }
  }

  public void executeBatch() throws SQLException {
    this.preparedStatement.executeLargeBatch();
  }

  @Override
  public void setBoolean(boolean value) {
    withException(() -> this.preparedStatement.setBoolean(getNextIndex(), value));
  }

  @Override
  public void setByte(byte value) {
    withException(() -> this.preparedStatement.setByte(getNextIndex(), value));
  }

  @Override
  public void setShort(short value) {
    withException(() -> this.preparedStatement.setShort(getNextIndex(), value));
  }

  @Override
  public void setInt(int value) {
    withException(() -> this.preparedStatement.setInt(getNextIndex(), value));
  }

  @Override
  public void setLong(long value) {
    withException(() -> this.preparedStatement.setLong(getNextIndex(), value));
  }

  @Override
  public void setFloat(float value) {
    withException(() -> this.preparedStatement.setFloat(getNextIndex(), value));
  }

  @Override
  public void setDouble(double value) {
    withException(() -> this.preparedStatement.setDouble(getNextIndex(), value));
  }

  @Override
  public void setBigDecimal(BigDecimal value) {
    withException(() -> this.preparedStatement.setBigDecimal(getNextIndex(), value));
  }

  @Override
  public void setString(String value) {
    withException(() -> this.preparedStatement.setString(getNextIndex(), value));
  }

  @Override
  public void setBytes(byte[] value) {
    withException(() -> this.preparedStatement.setBytes(getNextIndex(), value));
  }

  @Override
  public void setDate(Date value) {
    withException(() -> {
      if (this.timeZone != null) {
        this.preparedStatement.setDate(getNextIndex(), value, Calendar.getInstance(this.timeZone));
      } else {
        this.preparedStatement.setDate(getNextIndex(), value);
      }
    });
  }

  @Override
  public void setTimestamp(Timestamp value) {
    withException(() -> {
      if (this.timeZone != null) {
        this.preparedStatement.setTimestamp(getNextIndex(), value, Calendar.getInstance(this.timeZone));
      } else {
        this.preparedStatement.setTimestamp(getNextIndex(), value);
      }
    });
  }

  @Override
  public void setObject(Object value) {
    withException(() -> this.preparedStatement.setObject(getNextIndex(), value));
  }

  @Override
  public void setNull(int typeId) {
    withException(() -> this.preparedStatement.setNull(getNextIndex(), typeId));
  }

  @Override
  public void close() throws SQLException {
    Connection connection = this.preparedStatement.getConnection();
    this.preparedStatement.close();
    connection.close();
  }

  private int getNextIndex() {
    return ++currentParameterIndex;
  }

  private void addError(SQLException e) {
    errors.add("SQLState(" + e.getSQLState() + ") vendor code(" + e.getErrorCode() + ") : " + e.getMessage());
  }

  private void withException(SQLExceptionRunnable runnable) {
    try {
      runnable.run();
    } catch (SQLException e) {
      addError(e);
    }
  }

}
