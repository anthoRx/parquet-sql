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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types.PrimitiveBuilder;

import java.util.HashMap;

public class SqlTypeMapping {

  @FunctionalInterface
  private interface TypeMappingFunction {
    PrimitiveType map(SQLColumnDefinition sqlField) throws ConvertException;
  }

  private static HashMap<String, TypeMappingFunction> mappers = new HashMap<String, TypeMappingFunction>() {{
    put("java.lang.String", SqlTypeMapping::mapString);
    put("java.sql.Timestamp", SqlTypeMapping::mapTimestamp);
    put("oracle.sql.TIMESTAMP", SqlTypeMapping::mapTimestamp);
    put("java.math.BigDecimal", SqlTypeMapping::mapBigDecimal);
    put("java.lang.Double", SqlTypeMapping::mapDouble);
  }};


  public static PrimitiveType getPrimitiveType(SQLColumnDefinition sqlColumnDefinition) throws ConvertException {
    TypeMappingFunction mapper = mappers.get(sqlColumnDefinition.getColumnTypeName());
    if (mapper == null) {
      throw new ConvertException(String.format("impossible to map type %s. \"%s\" mapper is not implemented",
          sqlColumnDefinition.getName(), sqlColumnDefinition.getColumnTypeName()));
    }
    return mapper.map(sqlColumnDefinition);
  }

  private static PrimitiveType mapString(SQLColumnDefinition sqlMetaField) {
    return createPrimitiveType(sqlMetaField.getName(),
        PrimitiveType.PrimitiveTypeName.BINARY,
        LogicalTypeAnnotation.stringType(),
        sqlMetaField.isNullable());
  }

  private static PrimitiveType mapTimestamp(SQLColumnDefinition sqlMetaField) {
    return createPrimitiveType(sqlMetaField.getName(),
        PrimitiveType.PrimitiveTypeName.INT64,
        LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
        sqlMetaField.isNullable());
  }

  private static PrimitiveType mapBigDecimal(SQLColumnDefinition sqlMetaField) {
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
    return createPrimitiveType(sqlMetaField.getName(),
        primitiveTypeName,
        LogicalTypeAnnotation.decimalType(checkedScale, checkedPrecision),
        sqlMetaField.isNullable());
  }

  private static PrimitiveType mapDouble(SQLColumnDefinition sqlMetaField) {
    return createPrimitiveType(sqlMetaField.getName(),
        PrimitiveType.PrimitiveTypeName.DOUBLE,
        null,
        sqlMetaField.isNullable());
  }

  private static PrimitiveType createPrimitiveType(String name,
                                                   PrimitiveType.PrimitiveTypeName primitiveTypeName,
                                                   LogicalTypeAnnotation logicalTypeAnnotation,
                                                   Boolean nullable) {
    PrimitiveBuilder<PrimitiveType> primitiveBuilder;
    if (nullable) {
      primitiveBuilder = org.apache.parquet.schema.Types.optional(primitiveTypeName);
    } else {
      primitiveBuilder = org.apache.parquet.schema.Types.required(primitiveTypeName);
    }

    if (logicalTypeAnnotation != null) {
      primitiveBuilder.as(logicalTypeAnnotation);
    }

    return primitiveBuilder.named(name);
  }
}
