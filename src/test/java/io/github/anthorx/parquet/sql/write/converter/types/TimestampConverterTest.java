package io.github.anthorx.parquet.sql.write.converter.types;

import io.github.anthorx.parquet.sql.model.SQLField;
import io.github.anthorx.parquet.sql.record.RecordField;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimestampConverterTest {

  // 15/09/2020 10:20:36 PM
  private long refUtcTimestamp = 1600208436;
  private long MILLIS_BY_HOUR = 3600000;

  private TimestampConverter converter = new TimestampConverter();

  @Test
  public void convertUtcTimestamp() {
    TimeZone.setDefault(TimeZone.getTimeZone("Etc/UTC"));
    SQLField field = new SQLField("tsp", new Timestamp(refUtcTimestamp));
    RecordField<?> record = converter.convert(field);

    assertTrue(record.getValue() instanceof Long);
    long resultTsp = (Long) record.getValue();

    assertEquals(refUtcTimestamp, resultTsp);
  }


  @Test
  public void convertGMTPlus2Timestamp() {
    TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT+2"));
    SQLField field = new SQLField("tsp", new Timestamp(refUtcTimestamp));
    RecordField<?> record = converter.convert(field);

    assertTrue(record.getValue() instanceof Long);
    long resultTsp = (Long) record.getValue();

    long refPlus2Timestamp = refUtcTimestamp - MILLIS_BY_HOUR * 2;
    assertEquals(refPlus2Timestamp, resultTsp);
  }


  @Test
  public void convertGMTMinus2Timestamp() {
    TimeZone.setDefault(TimeZone.getTimeZone("Etc/GMT-2"));
    SQLField field = new SQLField("tsp", new Timestamp(refUtcTimestamp));
    RecordField<?> record = converter.convert(field);

    assertTrue(record.getValue() instanceof Long);
    long resultTsp = (Long) record.getValue();

    long refMinus2Timestamp = refUtcTimestamp + MILLIS_BY_HOUR * 2;
    assertEquals(refMinus2Timestamp, resultTsp);
  }
}
