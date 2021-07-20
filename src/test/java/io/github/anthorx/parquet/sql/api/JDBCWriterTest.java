package io.github.anthorx.parquet.sql.api;

import org.junit.jupiter.api.Assertions;
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
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JDBCWriterTest {

  @Mock
  private DataSource dataSource;

  @Mock
  private Connection connection;

  @Mock
  private PreparedStatement preparedStatement;

  @Test
  public void insertQueryGeneratedFromListOfColumns() {
    String result = JDBCWriter.prepareStatementQuery("car", Arrays.asList("brand", "color", "price"));

    String expected = "insert into car(brand,color,price) values (?,?,?)";

    Assertions.assertEquals(expected, result);
  }

  @Test
  public void initializeRecordConsumerFromListOfColumns() throws SQLException {
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    JDBCWriter recordConsumer = new JDBCWriter(dataSource, "car", Arrays.asList("brand", "color", "price"));

    Mockito.verify(connection).prepareStatement("insert into car(brand,color,price) values (?,?,?)");
    Assertions.assertNotNull(recordConsumer);
  }

  @Test
  public void initializeWithListOfColumnsShouldThrowException() throws SQLException {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      new JDBCWriter(dataSource, "car", Collections.emptyList());
    });
  }

}
