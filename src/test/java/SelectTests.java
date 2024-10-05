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
import physical_operator.Operator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

    URI queriesFile = Objects.requireNonNull(classLoader.getResource("samples/input/queries.sql")).toURI();

    statements = CCJSqlParserUtil.parseStatements(Files.readString(Paths.get(queriesFile)));
    queryPlanBuilder = new QueryPlanBuilder();
    statementList = statements.getStatements();
  }

  /**
   * Test the select statement for sailors tables - "<"
   * 
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testSelectSailors1() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 0));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 499;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples = new Tuple[] {
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
   * Test the select statement for sailors tables - ">"
   * 
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testSelectSailors2() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 1));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 497;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples = new Tuple[] {
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
   * Test the select statement for sailors tables - "="
   * 
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testSelectSailors3() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 2));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 4;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 4 tuple
    Tuple[] expectedFirstThreeTuples = new Tuple[] {
        new Tuple(new ArrayList<>(Arrays.asList(47, 47, 73))),
        new Tuple(new ArrayList<>(Arrays.asList(147, 147, 111))),
        new Tuple(new ArrayList<>(Arrays.asList(8, 8, 84))),
        new Tuple(new ArrayList<>(Arrays.asList(78, 78, 50)))
    };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the select statement for sailors tables - "<="
   * 
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testSelectSailors4() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 3));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 503;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples = new Tuple[] {
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
   * Test the select statement for sailors tables - ">="
   * 
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testSelectSailors5() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 4));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 501;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 4 tuple
    Tuple[] expectedFirstThreeTuples = new Tuple[] {
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
   * Test the select statement for sailors tables - "<>"
   * 
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testSelectSailors6() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 5));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 996;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 4 tuple
    Tuple[] expectedFirstThreeTuples = new Tuple[] {
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

  /**
   * Test the select statement for sailors tables - "TRUE AND TRUE"
   * 
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testSelectSailors7() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 6));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples = new Tuple[] {
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

  /**
   * Test the select statement for sailors tables - "TRUE AND FALSE"
   * 
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testSelectSailors8() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 7));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");
  }

  /**
   * Test the select statement for sailors tables - "FALSE AND FALSE"
   * 
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testSelectSailors9() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 8));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 0;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

  }
}
