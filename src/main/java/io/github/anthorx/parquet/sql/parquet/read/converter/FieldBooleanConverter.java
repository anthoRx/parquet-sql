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
import io.github.anthorx.parquet.sql.parquet.model.RecordField;

import java.util.function.Consumer;

/**
 * Converter for boolean values.
 * Primitive types : BOOLEAN
 * Logical Types : None
 */
public class FieldBooleanConverter extends FieldConverter<Boolean> {

  public FieldBooleanConverter(Consumer<RecordField<Boolean>> f, String fieldName) {
    super(f, fieldName, ReadRecordConsumer::setBoolean);
  }

  @Override
  public void addBoolean(boolean value) {
    acceptNewReadRecordFromValue(value);
  }

}