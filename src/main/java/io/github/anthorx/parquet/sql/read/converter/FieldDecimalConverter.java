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

package io.github.anthorx.parquet.sql.read.converter;

import io.github.anthorx.parquet.sql.record.ReadRecordConsumer;
import io.github.anthorx.parquet.sql.record.RecordField;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Consumer;

/**
 * Converter for decimal values.
 * Logical Types : Decimal
 * Primitive types : INT32, INT64, FIXED_LEN_BYTE_ARRAY, BINARY
 */
public class FieldDecimalConverter extends FieldConverter<BigDecimal> {

  private final int scale;

  public FieldDecimalConverter(Consumer<RecordField<BigDecimal>> f, String fieldName, PrimitiveType primitiveType) {
    super(f, fieldName, primitiveType, ReadRecordConsumer::setBigDecimal);
    this.scale = primitiveType.getDecimalMetadata().getScale();
  }

  @Override
  public void addBinary(Binary value) {
    BigDecimal bd = new BigDecimal(new BigInteger(value.getBytes()), scale);
    acceptNewReadRecordFromValue(bd);
  }

  @Override
  public void addLong(long value) {
    acceptNewReadRecordFromValue(new BigDecimal(BigInteger.valueOf(value), this.scale));
  }

  @Override
  public void addInt(int value) {
    acceptNewReadRecordFromValue(new BigDecimal(BigInteger.valueOf(value), this.scale));
  }
}
