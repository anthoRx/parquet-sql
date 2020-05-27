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

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.arx.parquet.sql.model.SQLMetaField;

import java.sql.Types;
import java.util.HashMap;

public class SqlTypeMapping {

  @FunctionalInterface
  private interface TypeMappingFunction {
    PrimitiveType map(SQLMetaField sqlField) throws ConvertException;
  }

  static private HashMap<String, TypeMappingFunction> mappers = new HashMap<String, TypeMappingFunction>() {{
    put("java.lang.String", SqlTypeMapping::mapString);
    put("java.sql.Timestamp", SqlTypeMapping::mapTimestamp);
    put("oracle.sql.TIMESTAMP", SqlTypeMapping::mapTimestamp);
    put("java.math.BigDecimal", SqlTypeMapping::mapBigDecimal);
    put("java.lang.Double", SqlTypeMapping::mapDouble);
  }};

  static private PrimitiveType mapString(SQLMetaField sqlMetaField) {
    return org.apache.parquet.schema.Types
        .optional(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.stringType())
        .named(sqlMetaField.getName());
  }

  static private PrimitiveType mapTimestamp(SQLMetaField sqlMetaField) {
    return org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
        .as(OriginalType.TIMESTAMP_MILLIS)
        .named(sqlMetaField.getName());
  }
  static private PrimitiveType mapBigDecimal(SQLMetaField sqlMetaField) {
    int checkedPrecision = sqlMetaField.getPrecision() <= 0 ? 18 : sqlMetaField.getPrecision();
    int checkedScale = Math.max(sqlMetaField.getScale(), 0);

    PrimitiveType.PrimitiveTypeName primitiveTypeName;
    if (checkedPrecision <= 9) {
      primitiveTypeName = PrimitiveType.PrimitiveTypeName.INT32;
    } else if (checkedPrecision <= 18) {
      primitiveTypeName = PrimitiveType.PrimitiveTypeName.INT64;
    } else {
      primitiveTypeName = PrimitiveType.PrimitiveTypeName.BINARY;
    }
    return org.apache.parquet.schema.Types
        .optional(primitiveTypeName)
        .as(LogicalTypeAnnotation.decimalType(checkedScale, checkedPrecision))
        .named(sqlMetaField.getName());
  }

  static private PrimitiveType mapDouble(SQLMetaField sqlMetaField) {
    return createPrimitiveType(sqlMetaField.getName(), PrimitiveType.PrimitiveTypeName.DOUBLE, sqlMetaField.isNullable());
  }

  public static PrimitiveType getPrimitiveType(SQLMetaField sqlMetaField) throws ConvertException {
    TypeMappingFunction mapper = mappers.get(sqlMetaField.getColumnTypeName());
    if (mapper == null) {
      throw new ConvertException(String.format("impossible to map type %s. \"%s\" mapper is not implemented",
          sqlMetaField.getName(), sqlMetaField.getColumnTypeName()));
    }
    return mapper.map(sqlMetaField);
  }

  /**
   * Beign refactored
   */
  public static PrimitiveType getPrimitiveTypeFromType(SQLMetaField sqlMetaField) {
    switch (sqlMetaField.getColumnType()) {

      case Types.BOOLEAN:
        return createPrimitiveType(sqlMetaField.getName(), PrimitiveType.PrimitiveTypeName.BOOLEAN, sqlMetaField.isNullable());

      case Types.TINYINT:             // 1 byte
      case Types.SMALLINT:            // 2 bytes
      case Types.INTEGER:             // 4 bytes
        return createPrimitiveType(sqlMetaField.getName(), PrimitiveType.PrimitiveTypeName.INT32, sqlMetaField.isNullable());

      case Types.ROWID:
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.SQLXML:
        return createPrimitiveType(sqlMetaField.getName(), PrimitiveType.PrimitiveTypeName.BINARY, sqlMetaField.isNullable());  // unicode string

      case Types.REAL:                // Approximate numerical (mantissa single precision 7)
        return createPrimitiveType(sqlMetaField.getName(), PrimitiveType.PrimitiveTypeName.FLOAT, sqlMetaField.isNullable());   // A 32-bit IEEE single-float

      case Types.DOUBLE:              // Approximate numerical (mantissa precision 16)
        return createPrimitiveType(sqlMetaField.getName(), PrimitiveType.PrimitiveTypeName.DOUBLE, sqlMetaField.isNullable());

      case Types.DECIMAL:             // Exact numerical (5 - 17 bytes)
      case Types.NUMERIC:             // Exact numerical (5 - 17 bytes)
        int checkedPrecision = sqlMetaField.getPrecision() <= 0 ? 18 : sqlMetaField.getPrecision();
        int checkedScale = Math.max(sqlMetaField.getScale(), 0);

        PrimitiveType.PrimitiveTypeName primitiveTypeName;
        if (checkedPrecision <= 9) {
          primitiveTypeName = PrimitiveType.PrimitiveTypeName.INT32;
        } else if (checkedPrecision <= 18) {
          primitiveTypeName = PrimitiveType.PrimitiveTypeName.INT64;
        } else {
          primitiveTypeName = PrimitiveType.PrimitiveTypeName.BINARY;
        }
        return org.apache.parquet.schema.Types
            .optional(primitiveTypeName)
            .as(LogicalTypeAnnotation.decimalType(checkedScale, checkedPrecision))
            .named(sqlMetaField.getName());

      case Types.FLOAT:               // Approximate numerical (mantissa precision 16)
        return createPrimitiveType(sqlMetaField.getName(), PrimitiveType.PrimitiveTypeName.DOUBLE, sqlMetaField.isNullable());  // A 64-bit IEEE double-float


      case Types.DATE:
        return org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT64).as(OriginalType.DATE).named(sqlMetaField.getName());
//        return createPrimitiveType(name, PrimitiveType.PrimitiveTypeName.INT64, nullable); // 64 bit (signed)
      case Types.TIME:
      case Types.TIMESTAMP:
      case Types.TIME_WITH_TIMEZONE:
      case Types.TIMESTAMP_WITH_TIMEZONE:
      case Types.BIGINT:              // 8 bytes
        return org.apache.parquet.schema.Types.optional(PrimitiveType.PrimitiveTypeName.INT64).as(OriginalType.TIMESTAMP_MILLIS).named(sqlMetaField.getName());
//        return createPrimitiveType(name, PrimitiveType.PrimitiveTypeName.INT64, nullable); // 64 bit (signed)

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
      case Types.REF_CURSOR:
        return createPrimitiveType(sqlMetaField.getName(), PrimitiveType.PrimitiveTypeName.BINARY, sqlMetaField.isNullable()); // sequence of bytes
    }

    return createPrimitiveType(sqlMetaField.getName(), PrimitiveType.PrimitiveTypeName.BINARY, sqlMetaField.isNullable());
  }

  static private PrimitiveType createPrimitiveType(String name, PrimitiveType.PrimitiveTypeName primitiveTypeName, Boolean nullable) {
    PrimitiveType primitiveType;
    if (nullable) {
      primitiveType = org.apache.parquet.schema.Types.optional(primitiveTypeName).named(name);
    } else {
      primitiveType = org.apache.parquet.schema.Types.required(primitiveTypeName).named(name);
    }
    return primitiveType;
  }
}
