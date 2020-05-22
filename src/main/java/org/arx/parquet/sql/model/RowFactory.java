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

package org.arx.parquet.sql.model;

import org.arx.parquet.sql.converter.ConvertException;
import org.arx.parquet.sql.converter.RecordFieldConverter;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RowFactory {

  public static Row createRow(ResultSet rs) throws SQLException, ConvertException {
    int nbColumns = rs.getMetaData().getColumnCount();
    Row row = new Row();

    for (int index = 1; index <= nbColumns; index++) {
      SQLField sqlField = new SQLField(
          rs.getMetaData().getColumnName(index),
          rs.getObject(index),
          rs.getMetaData().getColumnType(index),
          rs.getMetaData().getPrecision(index),
          rs.getMetaData().getScale(index)
      );
      RecordField<?> field = new RecordFieldConverter().convert(sqlField);
      row.addField(field);
    }

    return row;
  }
}
