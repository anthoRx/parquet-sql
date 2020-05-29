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

package io.github.anthorx.parquet.sql.write;

import io.github.anthorx.parquet.sql.model.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class SQLWriteSupport extends WriteSupport<Row> {

  private final MessageType messageType;

  private RecordConsumer recordConsumer;

  public SQLWriteSupport(MessageType messageType) {
    this.messageType = messageType;
  }

  @Override
  public WriteContext init(Configuration configuration) {
    Map<String, String> extraMetaData = new HashMap<>();
    return new WriteContext(messageType, extraMetaData);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(Row row) {
    recordConsumer.startMessage();
    writeRow(row);
    recordConsumer.endMessage();
  }

  private void writeRow(Row row) {
    IntStream
        .range(0, row.getFields().size())
        .forEach(index -> {
          if (row.getField(index).isNotNull()) {
            recordConsumer.startField(row.getField(index).getName(), index);
            row.getField(index).apply(recordConsumer);
            recordConsumer.endField(row.getField(index).getName(), index);
          }
        });
  }
}