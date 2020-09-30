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

import java.sql.Date;
import java.util.function.Consumer;

/**
 * Converter for date values.
 * Logical Types : Date
 * Primitive types : INT32
 */
public class FieldDateConverter extends FieldConverter<Date> {

  private final long MILLIS_PER_DAY = 86400000;

  public FieldDateConverter(Consumer<RecordField<Date>> f, String fieldName) {
    super(f, fieldName, ReadRecordConsumer::setDate);
  }

  /**
   * @param value value to set
   */
  public void addInt(int value) {
    acceptNewReadRecordFromValue(new Date(value * MILLIS_PER_DAY));
  }
}
