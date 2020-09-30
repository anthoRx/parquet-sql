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
import java.sql.SQLException;

public class SQLRowFactory {

  public static SQLRow createSQLRow(ResultSet rs) throws SQLException, ConvertException {
    int nbColumns = rs.getMetaData().getColumnCount();
    SQLRow row = new SQLRow();

    for (int index = 1; index <= nbColumns; index++) {
      SQLField sqlField = new SQLField(
          rs.getMetaData().getColumnName(index),
          rs.getObject(index),
          rs.getMetaData().getColumnType(index),
          rs.getMetaData().getPrecision(index),
          rs.getMetaData().getScale(index),
          rs.getMetaData().getColumnClassName(index));

      row.addField(sqlField);
    }

    return row;
  }
}
