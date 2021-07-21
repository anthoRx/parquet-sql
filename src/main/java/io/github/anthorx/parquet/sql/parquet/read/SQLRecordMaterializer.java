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

package io.github.anthorx.parquet.sql.parquet.read;

import io.github.anthorx.parquet.sql.parquet.model.Record;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class SQLRecordMaterializer extends RecordMaterializer<Record> {

  private final SQLGroupConverter sqlGroupConverter;

  public SQLRecordMaterializer(MessageType parquetSchema) {
    this.sqlGroupConverter = new SQLGroupConverter(parquetSchema);
  }

  @Override
  public Record getCurrentRecord() {
    return sqlGroupConverter.getCurrentSQLRowRead();
  }

  @Override
  public GroupConverter getRootConverter() {
    return sqlGroupConverter;
  }
}
