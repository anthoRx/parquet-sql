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

import oracle.sql.TIMESTAMP;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.arx.parquet.sql.model.RecordField;
import org.arx.parquet.sql.model.SQLField;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

public class RecordFieldConverter implements Converter<SQLField, RecordField<?>> {

  @Override
  public RecordField<?> convert(SQLField sqlField) throws ConvertException {
    if (sqlField.getValue() == null) {
      return new RecordField(sqlField.getName(), null, (r, a) -> {
      });
    }

    switch (sqlField.getSqlType()) {
      case Types.BOOLEAN:
        return new RecordField<>(sqlField.getName(), (boolean) sqlField.getValue(), RecordConsumer::addBoolean);

      case Types.TINYINT:             // 1 byte
      case Types.SMALLINT:            // 2 bytes
      case Types.INTEGER:             // 4 bytes
        return new RecordField<>(sqlField.getName(), (int) sqlField.getValue(), RecordConsumer::addInteger);

      case Types.ROWID:
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.SQLXML:
        String value;
        if (sqlField.getValue() instanceof char[]) {
          value = new String((char[]) sqlField.getValue());
        } else {
          value = (String) sqlField.getValue();
        }
        Binary binaryString = Binary.fromString(value);
        return new RecordField<>(sqlField.getName(), binaryString, RecordConsumer::addBinary);

      case Types.REAL:                // Approximate numerical (mantissa single precision 7)
        return new RecordField<>(sqlField.getName(), (float) sqlField.getValue(), RecordConsumer::addFloat);  // A 32-bit IEEE single-float

      case Types.DOUBLE:              // Approximate numerical (mantissa precision 16)
      case Types.FLOAT:
        return new RecordField<>(sqlField.getName(), (double) sqlField.getValue(), RecordConsumer::addDouble);

      case Types.BIGINT:              // 8 bytes
        return new RecordField<>(sqlField.getName(), (long) sqlField.getValue(), RecordConsumer::addLong);

      case Types.DECIMAL:             // Exact numerical (5 - 17 bytes)
      case Types.NUMERIC:             // Exact numerical (5 - 17 bytes)
        int checkedPrecision = sqlField
            .getPrecision()
            .filter(p -> p > 0)
            .orElse(18);
        BigDecimal bd = (BigDecimal) sqlField.getValue();

        if (checkedPrecision <= 9) {
          return new RecordField<>(sqlField.getName(), bd.intValue(), RecordConsumer::addInteger);
        } else if (checkedPrecision <= 18) {
          return new RecordField<>(sqlField.getName(), bd.longValue(), RecordConsumer::addLong);
        } else {
          byte[] bdBytes = bd.unscaledValue().toByteArray();
          Binary binaryArray = Binary.fromReusedByteArray(bdBytes);
          return new RecordField<>(sqlField.getName(), binaryArray, RecordConsumer::addBinary);
        }

      case Types.DATE:
        Date date = (Date) sqlField.getValue();
        return new RecordField<>(sqlField.getName(), date.getTime(), RecordConsumer::addLong);

      case Types.TIME:
      case Types.TIMESTAMP:
      case Types.TIME_WITH_TIMEZONE:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        Timestamp timestamp;
        if (sqlField.getValue() instanceof TIMESTAMP) {
          try {
            TIMESTAMP t = (TIMESTAMP) sqlField.getValue();
            timestamp = t.timestampValue();
          } catch (SQLException e) {
            throw new ConvertException("impossible to convert " + sqlField.getName() + " field. Oracle timestamp can't be converted to Java timestamp", e);
          }
        } else {
          timestamp = (Timestamp) sqlField.getValue();
        }
        return new RecordField<>(sqlField.getName(), timestamp.getTime(), RecordConsumer::addLong);

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.NULL:
      case Types.OTHER:
      case Types.JAVA_OBJECT:
      case Types.DISTINCT:
      case Types.STRUCT:
      case Types.ARRAY:
      case Types.BLOB:
      case Types.CLOB:
      case Types.REF:
      case Types.DATALINK:
      case Types.NCLOB:
      case Types.REF_CURSOR: // sequence of bytes
      default:
        Binary binaryArray = Binary.fromConstantByteArray((byte[]) sqlField.getValue());
        return new RecordField<>(sqlField.getName(), binaryArray, RecordConsumer::addBinary);
    }
  }
}
