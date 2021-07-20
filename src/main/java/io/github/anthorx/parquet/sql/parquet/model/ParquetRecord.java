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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ParquetRecord {

  List<ParquetRecordField<?>> fields = new ArrayList<>();

  public ParquetRecord() {
  }

  public <T> void addField(ParquetRecordField<T> field) {
    fields.add(field);
  }

  public List<ParquetRecordField<?>> getFields() {
    return fields;
  }


  public ParquetRecordField<?> getField(int index) {
    return fields.get(index);
  }

  public Optional<ParquetRecordField<?>> getField(String name) {
    return fields
        .stream()
        .filter(rr -> rr.getName().equals(name))
        .findFirst();
  }

  @Override
  public String toString() {
    return "Record{" +
        "fields=" + fields +
        '}';
  }
}
