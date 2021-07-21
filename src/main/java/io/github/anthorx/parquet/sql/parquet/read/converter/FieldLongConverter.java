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

import io.github.anthorx.parquet.sql.parquet.model.RecordField;
import io.github.anthorx.parquet.sql.jdbc.ReadRecordConsumer;

import java.util.function.Consumer;

/**
 * Converter for int values.
 * Logical Types : Signed and unsigned integer with bit width = 64
 * Primitive types : INT64
 */
public class FieldLongConverter extends FieldConverter<Long> {

  public FieldLongConverter(Consumer<RecordField<Long>> f, String fieldName) {
    super(f, fieldName, ReadRecordConsumer::setLong);
  }

  @Override
  public void addLong(long value) {
    acceptNewReadRecordFromValue(value);
  }
}