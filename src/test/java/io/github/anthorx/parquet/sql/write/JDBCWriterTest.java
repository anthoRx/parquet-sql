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

import io.github.anthorx.parquet.sql.read.PreparedStatementRecordConsumer;
import io.github.anthorx.parquet.sql.read.RecordConsumerInitializer;
import io.github.anthorx.parquet.sql.read.SQLParquetReaderWrapper;
import io.github.anthorx.parquet.sql.record.ReadRecordConsumer;
import io.github.anthorx.parquet.sql.record.Record;
import io.github.anthorx.parquet.sql.record.RecordField;
import junit.framework.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class JDBCWriterTest {


  @Mock
  private PreparedStatementRecordConsumer mockedRecordConsumer;

  @Mock
  private RecordConsumerInitializer lazyRecordConsumerInitializer;

  @Mock
  private SQLParquetReaderWrapper sqlParquetReaderWrapper;

  @BeforeEach
  public void setUp() throws SQLException {
    when(lazyRecordConsumerInitializer.initialize(anyList())).thenReturn(mockedRecordConsumer);
    when(sqlParquetReaderWrapper.getFieldsNames()).thenReturn(Arrays.asList("int", "string"));
  }

  private Record createBasicRecord() {
    Record record = new Record();
    record.addField(new RecordField<>("int", 10).addReadConsumer(ReadRecordConsumer::setInt));
    record.addField(new RecordField<>("string", "stringValue").addReadConsumer(ReadRecordConsumer::setString));
    return record;
  }


  @Test
  public void recordsAreInsertedUntilTheEnd() throws IOException, SQLException {
    when(sqlParquetReaderWrapper.read())
        .thenReturn(createBasicRecord(), createBasicRecord(), null);

    JDBCWriter jdbcWriter = spy(new JDBCWriter(lazyRecordConsumerInitializer, sqlParquetReaderWrapper, 2));
    jdbcWriter.write();

    verify(mockedRecordConsumer, times(2)).addBatch();
  }


  @Test
  public void recordFieldsAreConsumed() throws IOException, SQLException {
    when(sqlParquetReaderWrapper.read()).thenReturn(createBasicRecord(), null);

    doAnswer(invoc -> {
      int arg = invoc.getArgument(0);
      Assert.assertEquals(10, arg);
      return null;
    }).when(mockedRecordConsumer).setInt(anyInt());

    doAnswer(invoc -> {
      String arg = invoc.getArgument(0);
      Assert.assertEquals("stringValue", arg);
      return null;
    }).when(mockedRecordConsumer).setString(anyString());

    JDBCWriter jdbcWriter = new JDBCWriter(lazyRecordConsumerInitializer, sqlParquetReaderWrapper, 2);
    jdbcWriter.write();

    verify(mockedRecordConsumer, times(1)).addBatch();
  }

  @Test
  public void recordAreInsertedInBatchWhen() throws IOException, SQLException {
    when(sqlParquetReaderWrapper.read())
        .thenReturn(createBasicRecord(), createBasicRecord(), createBasicRecord(), null);

    JDBCWriter jdbcWriter = new JDBCWriter(lazyRecordConsumerInitializer, sqlParquetReaderWrapper, 2);
    jdbcWriter.write();

    verify(mockedRecordConsumer, times(2)).executeBatch();
  }

  @Test
  public void recordAreInsertedInBatch() throws IOException, SQLException {
    when(sqlParquetReaderWrapper.read())
        .thenReturn(createBasicRecord(), createBasicRecord(), createBasicRecord(), createBasicRecord(), null);

    JDBCWriter jdbcWriter = new JDBCWriter(lazyRecordConsumerInitializer, sqlParquetReaderWrapper, 2);
    jdbcWriter.write();

    verify(mockedRecordConsumer, times(2)).executeBatch();
  }

  @Test
  public void shouldSuccessInsertVaryingRecordColumns() throws IOException, SQLException {
    Record recordWithLessColumns = new Record();
    recordWithLessColumns.addField(new RecordField<>("int", 10).addReadConsumer(ReadRecordConsumer::setInt));
    when(sqlParquetReaderWrapper.read())
        .thenReturn(createBasicRecord(), createBasicRecord(), recordWithLessColumns, null);

    JDBCWriter jdbcWriter = new JDBCWriter(lazyRecordConsumerInitializer, sqlParquetReaderWrapper, 2);
    jdbcWriter.write();

    verify(mockedRecordConsumer, times(2)).executeBatch();
  }
}