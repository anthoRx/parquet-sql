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

package io.github.anthorx.parquet.sql.read;

import io.github.anthorx.parquet.sql.record.Record;
import io.github.anthorx.parquet.sql.record.RecordField;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Optional;

public class SQLParquetReaderTest {

  @Test
  public void convertParquetFileWithMultipleTypes() throws IOException {
    String filePath = getClass().getResource("/test.parquet").getPath();
    InputFile inputFile = HadoopInputFile.fromPath(new Path(filePath), new Configuration());

    ParquetReader<Record> parquetReader = SQLParquetReader
        .builder(inputFile)
        .build();

    Record record = parquetReader.read();

    assertField("timestamp", record, Timestamp.valueOf("2019-02-04 00:00:00"));
    assertField("date", record, Date.valueOf("2019-01-17"));
    assertField("bd_8_3", record, new BigDecimal("88.333").setScale(3));
    assertField("bd_10_5", record, new BigDecimal("1111.2222").setScale(5));
    assertField("bd_10_0", record, new BigDecimal("10.0").setScale(0));
    assertField("bd_20_12", record, new BigDecimal("111.9999999999").setScale(12));
    assertField("bd_38_18", record, new BigDecimal("1111.2222").setScale(18));
    assertField("float", record, new Float("11.22"));
    assertField("double", record, new Double("11.22"));
    assertField("string", record, "a string");
    assertField("int", record, 5);
    assertField("bool", record, true);
  }


  private <T> void assertField(String fieldName, Record record, T expectedValue) {
    Optional<RecordField<?>> field = record.getField(fieldName);

    Assert.assertTrue(field.isPresent());

    if (expectedValue instanceof Date) {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      String expected = df.format((Date) expectedValue);
      String actual = df.format((Date) field.get().getValue());
      Assert.assertEquals(expected, actual);
    } else {
      Assert.assertEquals(expectedValue, field.get().getValue());
    }
  }
}
