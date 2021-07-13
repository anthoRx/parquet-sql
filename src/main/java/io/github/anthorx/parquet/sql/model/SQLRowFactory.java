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

package io.github.anthorx.parquet.sql.model;

import io.github.anthorx.parquet.sql.write.converter.ConvertException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SQLRowFactory {

  private final SQLField[] columns;
  private final int columnCount;

  /**
   * An instance represents a table
   */
  public SQLRowFactory(ResultSetMetaData metaData) throws SQLException {
    columnCount = metaData.getColumnCount();

    columns = new SQLField[columnCount];
    for (int index = 1; index <= columnCount; index++) {
      columns[index - 1] = new SQLField(
          metaData.getColumnName(index),
          null,
          metaData.getColumnType(index),
          metaData.getPrecision(index),
          metaData.getScale(index),
          metaData.getColumnClassName(index));
    }
  }

  public SQLRow createSQLRow(ResultSet rs) throws SQLException {

    SQLRow row = new SQLRow();

    for (int index = 1; index <= columnCount; index++) {
      SQLField sqlField = columns[index - 1].copy(rs.getObject(index));

      row.addField(sqlField);
    }

    return row;
  }
}
