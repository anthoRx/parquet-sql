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

package io.github.anthorx.parquet.sql.parquet.write.converter.types;


import io.github.anthorx.parquet.sql.parquet.model.ParquetRecordField;
import io.github.anthorx.parquet.sql.jdbc.model.SQLColumnDefinition;
import io.github.anthorx.parquet.sql.jdbc.model.SQLField;
import io.github.anthorx.parquet.sql.parquet.write.converter.ConvertException;
import io.github.anthorx.parquet.sql.parquet.write.converter.PrimitiveTypeCreator;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;

public class BigDecimalConverter implements ParquetSQLConverter {

    public final static int DEFAULT_PRECISION = 18;

    @Override
    public boolean accept(Class<?> c) {
        return c.isAssignableFrom(BigDecimal.class);
    }

    @Override
    public ParquetRecordField<?> convert(SQLField sqlField) throws ConvertException {
        int checkedPrecision = sqlField
                .getPrecision()
                .filter(p -> p > 0)
                .orElse(DEFAULT_PRECISION);
        BigDecimal bd = (BigDecimal) sqlField.getValue();

        if (checkedPrecision <= 9) {
            return new ParquetRecordField<>(sqlField.getName(), bd.intValue())
                .addWriteConsumer(RecordConsumer::addInteger);
        } else if (checkedPrecision <= 18) {
            return new ParquetRecordField<>(sqlField.getName(), bd.longValue())
                .addWriteConsumer(RecordConsumer::addLong);
        } else {
            byte[] bdBytes = bd.unscaledValue().toByteArray();
            Binary binaryArray = Binary.fromReusedByteArray(bdBytes);
            return new ParquetRecordField<>(sqlField.getName(), binaryArray)
                .addWriteConsumer(RecordConsumer::addBinary);
        }
    }

    @Override
    public PrimitiveType convert(SQLColumnDefinition sqlColumnDefinition) throws ConvertException {
        int checkedPrecision = sqlColumnDefinition.getPrecision() <= 0 ? DEFAULT_PRECISION : sqlColumnDefinition.getPrecision();
        int checkedScale = Math.max(sqlColumnDefinition.getScale(), 0);

        PrimitiveType.PrimitiveTypeName primitiveTypeName;
        if (checkedPrecision <= 9) {
            primitiveTypeName = PrimitiveType.PrimitiveTypeName.INT32;
        } else if (checkedPrecision <= 18) {
            primitiveTypeName = PrimitiveType.PrimitiveTypeName.INT64;
        } else {
            primitiveTypeName = PrimitiveType.PrimitiveTypeName.BINARY;
        }
        return PrimitiveTypeCreator.create(sqlColumnDefinition.getName(),
                primitiveTypeName,
                LogicalTypeAnnotation.decimalType(checkedScale, checkedPrecision),
                sqlColumnDefinition.isNullable());
    }
}
