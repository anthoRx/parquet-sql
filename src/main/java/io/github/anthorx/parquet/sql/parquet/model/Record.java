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

package io.github.anthorx.parquet.sql.parquet.model;

import io.github.anthorx.parquet.sql.jdbc.NullRecordFieldConstructor;
import io.github.anthorx.parquet.sql.jdbc.ReadRecordConsumer;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Record {

  private final List<RecordField<?>> fields = new ArrayList<>();

  public <T> void addField(RecordField<T> field) {
    fields.add(field);
  }

  public List<RecordField<?>> getFields() {
    return fields;
  }


  public RecordField<?> getField(int index) {
    return fields.get(index);
  }

  public Optional<RecordField<?>> getField(String name) {
    return fields
        .stream()
        .filter(rr -> rr.getName().equals(name))
        .findFirst();
  }

  /**
   * read all the recordField in the schema order
   *
   * @param schemaField        used to define the record field order
   * @param readRecordConsumer the consumer used to consume the record fields
   */
  public void readAll(List<Type> schemaField, ReadRecordConsumer readRecordConsumer) {
    schemaField.stream()
        .map(type -> this
            .getField(type.getName())
            .orElseGet(() -> NullRecordFieldConstructor.newNullRecordField(type)))
        .forEach(recordField -> recordField.read(readRecordConsumer));
  }

  @Override
  public String toString() {
    return "Record{" +
        "fields=" + fields +
        '}';
  }
}
