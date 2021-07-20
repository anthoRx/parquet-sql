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

package io.github.anthorx.parquet.sql.jdbc;

// TODO: move to data-integration
//
//@ExtendWith(MockitoExtension.class)
//public class JDBCWriterTest {
//
//
//  @Mock
//  private JDBCWriter mockedRecordConsumer;
//
//  @Mock
//  private SQLParquetReader sqlParquetReader;
//
//  @BeforeEach
//  public void setUp() {
//    when(sqlParquetReader.getFieldsNames()).thenReturn(Arrays.asList("int", "string"));
//  }
//
//  private Record createBasicRecord() {
//    Record record = new Record();
//    record.addField(new RecordField<>("int", 10).addReadConsumer(ReadRecordConsumer::setInt));
//    record.addField(new RecordField<>("string", "stringValue").addReadConsumer(ReadRecordConsumer::setString));
//    return record;
//  }
//
//
//  @Test
//  public void recordsAreInsertedUntilTheEnd() throws IOException, SQLException {
//    when(sqlParquetReader.read())
//        .thenReturn(createBasicRecord(), createBasicRecord(), null);
//
//    JDBCWriter jdbcWriter = spy(new JDBCWriter(lazyRecordConsumerInitializer, sqlParquetReader, 2));
//    jdbcWriter.write();
//
//    verify(mockedRecordConsumer, times(2)).addBatch();
//  }
//
//
//  @Test
//  public void recordFieldsAreConsumed() throws IOException, SQLException {
//    when(sqlParquetReader.read()).thenReturn(createBasicRecord(), null);
//
//    JDBCWriter jdbcWriter = new JDBCWriter(lazyRecordConsumerInitializer, sqlParquetReader, 2);
//    jdbcWriter.write();
//
//    verify(mockedRecordConsumer, times(1)).addBatch();
//  }
//
//  @Test
//  public void recordAreInsertedInBatchWhen() throws IOException, SQLException {
//    when(sqlParquetReader.read())
//        .thenReturn(createBasicRecord(), createBasicRecord(), createBasicRecord(), null);
//
//    JDBCWriter jdbcWriter = new JDBCWriter(lazyRecordConsumerInitializer, sqlParquetReader, 2);
//    jdbcWriter.write();
//
//    verify(mockedRecordConsumer, times(2)).executeBatch();
//  }
//
//  @Test
//  public void recordAreInsertedInBatch() throws IOException, SQLException {
//    when(sqlParquetReader.read())
//        .thenReturn(createBasicRecord(), createBasicRecord(), createBasicRecord(), createBasicRecord(), null);
//
//    JDBCWriter jdbcWriter = new JDBCWriter(lazyRecordConsumerInitializer, sqlParquetReader, 2);
//    jdbcWriter.write();
//
//    verify(mockedRecordConsumer, times(2)).executeBatch();
//  }
//
//  @Test
//  public void shouldSuccessInsertVaryingRecordColumns() throws IOException, SQLException {
//    Record recordWithLessColumns = new Record();
//    recordWithLessColumns.addField(new RecordField<>("int", 10).addReadConsumer(ReadRecordConsumer::setInt));
//    when(sqlParquetReader.read())
//        .thenReturn(createBasicRecord(), createBasicRecord(), recordWithLessColumns, null);
//
//    JDBCWriter jdbcWriter = new JDBCWriter(lazyRecordConsumerInitializer, sqlParquetReader, 2);
//    jdbcWriter.write();
//
//    verify(mockedRecordConsumer, times(2)).executeBatch();
//  }
//}