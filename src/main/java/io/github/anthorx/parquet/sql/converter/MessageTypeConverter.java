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

package io.github.anthorx.parquet.sql.converter;

import io.github.anthorx.parquet.sql.model.SQLColumnDefinition;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MessageTypeConverter implements Converter<ResultSetMetaData, MessageType> {
  private final String schemaName;

  public MessageTypeConverter(String schemaName) {
    this.schemaName = schemaName;
  }

  public MessageType convert(ResultSetMetaData resultSetMetaData) throws ConvertException {
    List<Type> convertedTypes = new ArrayList<>();

    try {
      int columnCount = resultSetMetaData.getColumnCount();
      for (int index = 1; index <= columnCount; index++) {

        SQLColumnDefinition sqlColumnDefinition = new SQLColumnDefinition(
            resultSetMetaData.getColumnName(index),
            resultSetMetaData.getColumnType(index),
            resultSetMetaData.isNullable(index) == ResultSetMetaData.columnNullable,
            resultSetMetaData.getPrecision(index),
            resultSetMetaData.getScale(index),
            resultSetMetaData.getColumnClassName(index));

        PrimitiveType primitiveType = SqlTypeMapping.getPrimitiveType(sqlColumnDefinition);
        convertedTypes.add(primitiveType);
      }
    } catch (SQLException e) {
      throw new ConvertException("Error when converting ResultSetMetaData to MessageType.", e);
    }

    return new MessageType(this.schemaName, convertedTypes);
  }
}
