import common.DBCatalog;
import common.QueryPlanBuilder;
import common.Tuple;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import physical_operator.Operator;

public class SelectTests {

  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;
  private int index = 3;

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException, URISyntaxException {
    ClassLoader classLoader = SelectTests.class.getClassLoader();
    URI path = Objects.requireNonNull(classLoader.getResource("samples/input")).toURI();
    Path resourcePath = Paths.get(path);

    DBCatalog.getInstance().setDataDirectory(resourcePath.resolve("db").toString());

    URI queriesFile =
        Objects.requireNonNull(classLoader.getResource("samples/input/customized_queries.sql")).toURI();

    statements = CCJSqlParserUtil.parseStatements(Files.readString(Paths.get(queriesFile)));
    queryPlanBuilder = new QueryPlanBuilder();
    statementList = statements.getStatements();
  }

  /**
   * Test the select statement for Sailors table - "SELECT * FROM Sailors where Sailors.A <
   * Sailors.B"
   */
  @DisplayName("SELECT * FROM Sailors where Sailors.A < Sailors.B")
  @Test
  public void testSelectSailors1() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 0));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 499;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuples
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139))),
          new Tuple(new ArrayList<>(Arrays.asList(75, 191, 192))),
          new Tuple(new ArrayList<>(Arrays.asList(133, 197, 18)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the select statement for Sailors table - "SELECT * FROM Sailors where Sailors.A >
   * Sailors.B"
   */
  @DisplayName("SELECT * FROM Sailors where Sailors.A > Sailors.B")
  @Test
  public void testSelectSailors2() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 1));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 497;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuples
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(181, 128, 129))),
          new Tuple(new ArrayList<>(Arrays.asList(147, 45, 118))),
          new Tuple(new ArrayList<>(Arrays.asList(81, 1, 195)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the select statement for Sailors table - "SELECT * FROM Sailors where Sailors.A =
   * Sailors.B"
   */
  @DisplayName("SELECT * FROM Sailors where Sailors.A = Sailors.B")
  @Test
  public void testSelectSailors3() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 2));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 4;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 4 tuples
    Tuple[] expectedFirstFourTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(47, 47, 73))),
          new Tuple(new ArrayList<>(Arrays.asList(147, 147, 111))),
          new Tuple(new ArrayList<>(Arrays.asList(8, 8, 84))),
          new Tuple(new ArrayList<>(Arrays.asList(78, 78, 50)))
        };

    for (int i = 0; i < expectedFirstFourTuples.length; i++) {
      Tuple expectedTuple = expectedFirstFourTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the select statement for Sailors table - "SELECT * FROM Sailors where Sailors.A <=
   * Sailors.B"
   */
  @DisplayName("SELECT * FROM Sailors where Sailors.A <= Sailors.B")
  @Test
  public void testSelectSailors4() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 3));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 503;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuples
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139))),
          new Tuple(new ArrayList<>(Arrays.asList(75, 191, 192))),
          new Tuple(new ArrayList<>(Arrays.asList(133, 197, 18))),
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the select statement for Sailors table - "SELECT * FROM Sailors where Sailors.A >=
   * Sailors.B"
   */
  @DisplayName("SELECT * FROM Sailors where Sailors.A >= Sailors.B")
  @Test
  public void testSelectSailors5() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 4));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 501;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuples
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(181, 128, 129))),
          new Tuple(new ArrayList<>(Arrays.asList(147, 45, 118))),
          new Tuple(new ArrayList<>(Arrays.asList(81, 1, 195)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the select statement for Sailors table - "SELECT * FROM Sailors where Sailors.A <>
   * Sailors.B"
   */
  @DisplayName("SELECT * FROM Sailors where Sailors.A <> Sailors.B")
  @Test
  public void testSelectSailors6() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 5));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 996;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuples
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139))),
          new Tuple(new ArrayList<>(Arrays.asList(181, 128, 129))),
          new Tuple(new ArrayList<>(Arrays.asList(147, 45, 118)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 1=1 AND 1=1" */
  @DisplayName("SELECT * FROM Sailors where 1=1 AND 1=1")
  @Test
  public void testSelectSailors7() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 6));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuples
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139))),
          new Tuple(new ArrayList<>(Arrays.asList(181, 128, 129))),
          new Tuple(new ArrayList<>(Arrays.asList(147, 45, 118)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 1=1 AND 1=0" */
  @DisplayName("SELECT * FROM Sailors where 1=1 AND 1=0")
  @Test
  public void testSelectSailors8() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 7));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 1=0 AND 1=0" */
  @DisplayName("SELECT * FROM Sailors where 1=0 AND 1=0")
  @Test
  public void testSelectSailors9() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 8));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 1<2" */
  @DisplayName("SELECT * FROM Sailors where 1<2")
  @Test
  public void testSelectSailors10() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 9));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 3<2" */
  @DisplayName("SELECT * FROM Sailors where 3<2")
  @Test
  public void testSelectSailors11() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 10));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 1>2" */
  @DisplayName("SELECT * FROM Sailors where 1>2")
  @Test
  public void testSelectSailors12() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 11));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 3>2" */
  @DisplayName("SELECT * FROM Sailors where 3>2")
  @Test
  public void testSelectSailors13() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 12));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 1<=2" */
  @DisplayName("SELECT * FROM Sailors where 1<=2")
  @Test
  public void testSelectSailors14() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 13));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 3<=2" */
  @DisplayName("SELECT * FROM Sailors where 3<=2")
  @Test
  public void testSelectSailors15() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 14));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 1>=2" */
  @DisplayName("SELECT * FROM Sailors where 1>=2")
  @Test
  public void testSelectSailors16() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 15));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 3>=2" */
  @DisplayName("SELECT * FROM Sailors where 3>=2")
  @Test
  public void testSelectSailors17() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 16));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 1=2" */
  @DisplayName("SELECT * FROM Sailors where 1=2")
  @Test
  public void testSelectSailors18() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 17));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 1=1" */
  @DisplayName("SELECT * FROM Sailors where 1=1")
  @Test
  public void testSelectSailors19() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 18));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 1<>2" */
  @DisplayName("SELECT * FROM Sailors where 1<>2")
  @Test
  public void testSelectSailors20() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 19));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /** Test the select statement for Sailors table - "SELECT * FROM Sailors where 1<>1" */
  @DisplayName("SELECT * FROM Sailors where 1<>1")
  @Test
  public void testSelectSailors21() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 20));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }
}
