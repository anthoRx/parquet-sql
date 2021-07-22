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
import org.eclipse.jdt.internal.core.Assert;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.anthorx.parquet.sql.util.AssertionUtils.notEmpty;
import static io.github.anthorx.parquet.sql.util.AssertionUtils.notNull;

/**
 * use a prepareStatement to do batch executions on JDBC writer
 */
public class JDBCWriter implements IJDBCWriter {

  private final PreparedStatement preparedStatement;
  private final List<String> errors;
  private int currentParameterIndex = 0;

  /**
   * default constructor that should be used
   * If you want a custom prepare statement, use the other constructor
   */
  public JDBCWriter(DataSource dataSource, String tableName, Collection<String> columnNames) throws SQLException, IllegalArgumentException {
    this(dataSource.getConnection().prepareStatement(prepareStatementQuery(tableName, columnNames)));
  }

  /**
   * if you want a custom prepareStatement, use this constructor.
   * Otherwise for default behavior just provide datasource
   */
  public JDBCWriter(PreparedStatement preparedStatement) {
    this.preparedStatement = preparedStatement;
    this.errors = new ArrayList<>();
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

  @Override
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

  @Override
  public void executeBatch() throws SQLException {
    this.preparedStatement.executeLargeBatch();
  }

  private int getNextIndex() {
    return ++currentParameterIndex;
  }

  private void addError(SQLException e) {
    errors.add("SQLState(" + e.getSQLState() + ") vendor code(" + e.getErrorCode() + ") : " + e.getMessage());
  }

  @Override
  public void setBoolean(boolean value) {
    try {
      this.preparedStatement.setBoolean(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void setByte(byte value) {
    try {
      this.preparedStatement.setByte(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void setShort(short value) {
    try {
      this.preparedStatement.setShort(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void setInt(int value) {
    try {
      this.preparedStatement.setInt(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void setLong(long value) {
    try {
      this.preparedStatement.setLong(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void setFloat(float value) {
    try {
      this.preparedStatement.setFloat(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void setDouble(double value) {
    try {
      this.preparedStatement.setDouble(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void setBigDecimal(BigDecimal value) {
    try {
      this.preparedStatement.setBigDecimal(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void setString(String value) {
    try {
      this.preparedStatement.setString(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void setBytes(byte[] value) {
    try {
      this.preparedStatement.setBytes(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void setDate(Date value) {
    try {
      this.preparedStatement.setDate(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }


  @Override
  public void setTimestamp(Timestamp value) {
    try {
      this.preparedStatement.setTimestamp(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void setObject(Object value) {
    try {
      this.preparedStatement.setObject(getNextIndex(), value);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void setNull(int typeId) {
    try {
      this.preparedStatement.setNull(getNextIndex(), typeId);
    } catch (SQLException e) {
      addError(e);
    }
  }

  @Override
  public void close() throws SQLException {
    Connection connection = this.preparedStatement.getConnection();
    this.preparedStatement.close();
    connection.close();
  }
}
