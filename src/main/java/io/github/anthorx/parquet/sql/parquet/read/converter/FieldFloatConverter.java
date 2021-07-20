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

package io.github.anthorx.parquet.sql.parquet.read.converter;

import io.github.anthorx.parquet.sql.jdbc.ReadRecordConsumer;
import io.github.anthorx.parquet.sql.parquet.model.ParquetRecordField;

import java.util.function.Consumer;

/**
 * Converter for float values.
 * Logical Types : None
 * Primitive types : FLOAT
 */
public class FieldFloatConverter extends FieldConverter<Float> {

  public FieldFloatConverter(Consumer<ParquetRecordField<Float>> f, String fieldName) {
    super(f, fieldName, ReadRecordConsumer::setFloat);
  }

  @Override
  public void addFloat(float value) {
    acceptNewReadRecordFromValue(value);
  }
}