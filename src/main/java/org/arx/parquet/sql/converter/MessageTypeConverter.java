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

package org.arx.parquet.sql.converter;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MessageTypeConverter implements Converter<ResultSetMetaData, MessageType> {
  private static final Logger LOG = LoggerFactory.getLogger(MessageTypeConverter.class);
  private static final String DEFAULT_SCHEMA_NAME = "default_schema";

  private final String schemaName;

  public MessageTypeConverter(String schemaName) {
    this.schemaName = schemaName;
  }

  public MessageType convert(ResultSetMetaData resultSetMetaData) throws ConvertException {
    List<Type> convertedTypes = new ArrayList<>();

    try {
      int columnCount = resultSetMetaData.getColumnCount();
      for (int index = 1; index <= columnCount; index++) {

        String columnName = resultSetMetaData.getColumnName(index);
        int sqlColumnType = resultSetMetaData.getColumnType(index);
        boolean isNullable = resultSetMetaData.isNullable(index) == 1;
        int precision = resultSetMetaData.getPrecision(index);
        int scale = resultSetMetaData.getScale(index);


        PrimitiveType primitiveType = SqlTypeMapping.getPrimitiveType(columnName, sqlColumnType, true, precision, scale);
        convertedTypes.add(primitiveType);
      }
    } catch (SQLException e) {
      throw new ConvertException("Error when converting ResultSetMetaData to MessageType.", e);
    }

    return new MessageType(this.schemaName, convertedTypes);
  }
}
