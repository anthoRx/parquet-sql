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

import io.github.anthorx.parquet.sql.model.SQLRow;
import io.github.anthorx.parquet.sql.record.Record;
import io.github.anthorx.parquet.sql.write.converter.ConvertException;
import io.github.anthorx.parquet.sql.write.converter.ConverterContainer;
import io.github.anthorx.parquet.sql.write.converter.RecordsConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class SQLWriteSupport extends WriteSupport<SQLRow> {
  private static final Logger LOG = LoggerFactory.getLogger(SQLWriteSupport.class);

  private final MessageType messageType;

  private RecordConsumer recordConsumer;

  private final ConverterContainer converterContainer;

  public SQLWriteSupport(MessageType messageType, ConverterContainer converterContainer) {
    this.messageType = messageType;
    this.converterContainer = converterContainer;
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
  public void write(SQLRow row) {
    try {
      Record records = new RecordsConverter(converterContainer).convert(row);
      recordConsumer.startMessage();
      writeRecords(records);
      recordConsumer.endMessage();
    } catch (ConvertException e) {
      LOG.error("Can't convert " + row + " to Records. Row not written to the parquet file", e);
    }
  }

  private void writeRecords(Record records) {
    IntStream
        .range(0, records.getFields().size())
        .forEach(index -> {
          if (records.getField(index).isNotNull()) {
            recordConsumer.startField(records.getField(index).getName(), index);
            records.getField(index).applyWriteConsumer(recordConsumer);
            recordConsumer.endField(records.getField(index).getName(), index);
          }
        });
  }
}