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
import org.junit.jupiter.api.Test;
import physical_operator.Operator;

public class JoinTest {

  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;
  private int index = 32;

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException, URISyntaxException {
    ClassLoader classLoader = JoinTest.class.getClassLoader();
    URI path = Objects.requireNonNull(classLoader.getResource("samples/input")).toURI();
    Path resourcePath = Paths.get(path);

    DBCatalog.getInstance().setDataDirectory(resourcePath.resolve("db").toString());

    URI queriesFile =
        Objects.requireNonNull(classLoader.getResource("samples/input/customized_queries.sql"))
            .toURI();

    statements = CCJSqlParserUtil.parseStatements(Files.readString(Paths.get(queriesFile)));
    queryPlanBuilder = new QueryPlanBuilder();
    statementList = statements.getStatements();
  }

  /**
   * Test the join statement
   *
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testJoinSailors1() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 0));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139, 12, 143, 196))),
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139, 30, 63, 101))),
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139, 57, 24, 130)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the join statement
   *
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testJoinSailors2() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 1));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139, 64, 113, 139))),
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139, 181, 128, 129))),
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139, 147, 45, 118)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the join statement
   *
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testJoinSailors3() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 2));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 12000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(20, 156, 20, 128, 42, 12, 143, 196))),
          new Tuple(new ArrayList<>(Arrays.asList(20, 156, 20, 128, 42, 30, 63, 101))),
          new Tuple(new ArrayList<>(Arrays.asList(20, 156, 20, 128, 42, 57, 24, 130)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the join statement
   *
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testJoinSailors4() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 3));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 12;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(20, 156, 20, 128, 42, 1, 172, 99))),
          new Tuple(new ArrayList<>(Arrays.asList(20, 156, 20, 3, 173, 1, 172, 99))),
          new Tuple(new ArrayList<>(Arrays.asList(20, 156, 20, 135, 28, 1, 172, 99)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the join statement
   *
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testJoinSailors5() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 4));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 993928;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139, 181, 128, 129))),
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139, 147, 45, 118))),
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139, 81, 1, 195)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the join statement
   *
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testJoinSailors6() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 5));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 6072;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139, 64, 113, 139))),
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139, 64, 30, 176))),
          new Tuple(new ArrayList<>(Arrays.asList(64, 113, 139, 64, 4, 76)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }
}
