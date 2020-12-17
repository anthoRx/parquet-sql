package io.github.anthorx.parquet.sql.read;

import junit.framework.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;
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
  private DataSource dataSource;

  @Mock
  private Connection connection;

  @Mock
  private PreparedStatement preparedStatement;

  private final String tableName = "car";

  private RecordConsumerInitializer initializer;

  @BeforeEach
  public void setUp() {
    initializer = new RecordConsumerInitializer(dataSource, tableName);
  }

  @Test
  public void insertQueryGeneratedFromListOfColumns() {
    String result = initializer.insertQueryBuilder(Arrays.asList("brand", "color", "price"));

    String expected = "insert  into " + tableName + "(brand,color,price) values (?,?,?)";

    Assert.assertEquals(expected, result);
  }

  @Test
  public void initializeRecordConsumerFromListOfColumnsWithHint() {
    RecordConsumerInitializer init = new RecordConsumerInitializer(dataSource, tableName, "/*+ APPEND_VALUES */");
    String result = init.insertQueryBuilder(Arrays.asList("brand", "color", "price"));
    String expected = "insert /*+ APPEND_VALUES */ into " + tableName + "(brand,color,price) values (?,?,?)";

    Assert.assertEquals(expected, result);
  }


  @Test
  public void initializeRecordConsumerFromListOfColumns() throws SQLException {
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    PreparedStatementRecordConsumer recordConsumer = initializer.initialize(Arrays.asList("brand", "color", "price"));

    Mockito.verify(connection).prepareStatement("insert  into car(brand,color,price) values (?,?,?)");
    Assert.assertNotNull(recordConsumer);
  }

  @Test
  public void initializeWithListOfColumnsShouldThrowException() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      initializer.initialize(Collections.emptyList());
    });
  }

}
