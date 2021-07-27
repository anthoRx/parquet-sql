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

import io.github.anthorx.parquet.sql.jdbc.model.SQLField;
import io.github.anthorx.parquet.sql.jdbc.model.SQLRow;

import javax.sql.DataSource;
import java.sql.*;

public class JDBCReader implements AutoCloseable {

  private final SQLField[] columns;
  private final int columnCount;
  private final PreparedStatement preparedStatement;
  private final ResultSet resultSet;

  public JDBCReader(Connection connection, String tableName, int fetchSize) throws SQLException {

    preparedStatement = createPrepareStatement(connection, tableName, fetchSize);
    resultSet = preparedStatement.executeQuery();

    ResultSetMetaData metaData = resultSet.getMetaData();
    columnCount = metaData.getColumnCount();
    columns = getColumns(columnCount, metaData);
  }

  private static PreparedStatement createPrepareStatement(Connection connection, String tableName, int fetchSize) throws SQLException {
    String query = String.format("select * from %s", tableName);
    PreparedStatement result = connection.prepareStatement(query);
    result.setFetchSize(fetchSize);
    return result;
  }

  private static SQLField[] getColumns(int columnCount, ResultSetMetaData metaData) throws SQLException {
    SQLField[] result = new SQLField[columnCount];
    for (int index = 1; index <= columnCount; index++) {
      result[index - 1] = new SQLField(
          metaData.getColumnName(index),
          null,
          metaData.getColumnType(index),
          metaData.getPrecision(index),
          metaData.getScale(index),
          metaData.getColumnClassName(index));
    }
    return result;
  }

  public ResultSetMetaData getMetaData() throws SQLException {
      return this.resultSet.getMetaData();
}

  public SQLRow read() throws SQLException {
    if (resultSet.next()) {
      return createSqlRow(resultSet);
    } else {
      return null;
    }
  }

  private SQLRow createSqlRow(ResultSet resultSet) throws SQLException {
    SQLRow row = new SQLRow();

    for (int index = 1; index <= columnCount; index++) {
      SQLField sqlField = columns[index - 1].copy(resultSet.getObject(index));

      row.addField(sqlField);
    }

    return row;
  }

  @Override
  public void close() throws SQLException {
    Connection connection = this.preparedStatement.getConnection();
    this.preparedStatement.close();
    connection.close();
  }
}
