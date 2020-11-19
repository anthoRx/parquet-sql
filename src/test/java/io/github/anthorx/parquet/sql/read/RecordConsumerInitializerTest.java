package io.github.anthorx.parquet.sql.read;

import junit.framework.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class RecordConsumerInitializerTest {

  @Mock
  private Connection connection;
  private final String tableName = "car";

  private RecordConsumerInitializer initializer;

  @BeforeEach
  public void setUp() {
    initializer = new RecordConsumerInitializer(connection, tableName);
  }

  @Test
  public void insertQueryGeneratedFromListOfColumns() {
    String result = initializer.insertQueryBuilder(Arrays.asList("brand", "color", "price"));

    String expected = "insert into " + tableName + "(brand,color,price) values (?,?,?)";

    Assert.assertEquals(expected, result);
  }

  @Test
  public void initializeRecordConsumerFromListOfColumns() throws SQLException {
    when(connection.prepareStatement(anyString())).thenReturn(mock(PreparedStatement.class));
    PreparedStatementRecordConsumer recordConsumer = initializer.initialize(Arrays.asList("brand", "color", "price"));
    Assert.assertNotNull(recordConsumer);
  }

  @Test
  public void initializeWithListOfColumnsShouldThrowException() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      initializer.initialize(Collections.emptyList());
    });
  }

}
