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

import io.github.anthorx.parquet.sql.api.SQLParquetReader;
import io.github.anthorx.parquet.sql.parquet.model.ParquetRecord;
import io.github.anthorx.parquet.sql.parquet.model.ParquetRecordField;
import org.apache.hadoop.conf.Configuration;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SQLParquetReaderTest {

  @Test
  public void convertParquetFileWithMultipleTypes() throws IOException {
    String filePath = getClass().getResource("/test.parquet").getPath();

    SQLParquetReader sqlParquetReader = new SQLParquetReader(filePath, new Configuration());

    ParquetRecord parquetRecord = sqlParquetReader.read();

    assertField("timestamp", parquetRecord, Timestamp.valueOf("2019-02-04 00:00:00"));
    assertField("date", parquetRecord, Date.valueOf("2019-01-17"));
    assertField("bd_8_3", parquetRecord, new BigDecimal("88.333").setScale(3));
    assertField("bd_10_5", parquetRecord, new BigDecimal("1111.2222").setScale(5));
    assertField("bd_10_0", parquetRecord, new BigDecimal("10.0").setScale(0));
    assertField("bd_20_12", parquetRecord, new BigDecimal("111.9999999999").setScale(12));
    assertField("bd_38_18", parquetRecord, new BigDecimal("1111.2222").setScale(18));
    assertField("float", parquetRecord, new Float("11.22"));
    assertField("double", parquetRecord, new Double("11.22"));
    assertField("string", parquetRecord, "a string");
    assertField("int", parquetRecord, 5);
    assertField("bool", parquetRecord, true);
  }


  private <T> void assertField(String fieldName, ParquetRecord parquetRecord, T expectedValue) {
    Optional<ParquetRecordField<?>> field = parquetRecord.getField(fieldName);

    assertTrue(field.isPresent());

    if (expectedValue instanceof Date) {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      String expected = df.format((Date) expectedValue);
      String actual = df.format((Date) field.get().getValue());
      assertEquals(expected, actual);
    } else {
      assertEquals(expectedValue, field.get().getValue());
    }
  }

  @Test
  public void shouldSuccessReadSchemaSingleFile() throws IOException {
    String filePath = getClass().getResource("/test/part-00000-965fa2b7-87eb-40a5-853c-681c34cd733e-c000.snappy.parquet").getPath();
    SQLParquetReader sqlParquetReader = new SQLParquetReader(filePath, new Configuration());

    List<String> columns = sqlParquetReader.getFieldsNames();

    assertThat(columns, CoreMatchers.is(Arrays.asList("username", "value", "comment")));
  }
}
