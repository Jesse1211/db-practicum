import builder.QueryPlanBuilder;
import common.tuple.Tuple;
import compiler.DBCatalog;
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

public class SortTest {

  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;
  private int index = 28;

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException, URISyntaxException {
    ClassLoader classLoader = SortTest.class.getClassLoader();
    URI path = Objects.requireNonNull(classLoader.getResource("samples")).toURI();
    Path resourcePath = Paths.get(path);

    DBCatalog.getInstance()
        .setInterpreterConfig(resourcePath.resolve("interpreter_config_file.txt").toString());
    URI queriesFile =
        Objects.requireNonNull(classLoader.getResource("samples/input/customized_queries.sql"))
            .toURI();

    statements = CCJSqlParserUtil.parseStatements(Files.readString(Paths.get(queriesFile)));
    queryPlanBuilder = new QueryPlanBuilder();
    statementList = statements.getStatements();
  }

  /**
   * Test the sort statement for sailors tables - "single column"
   *
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testSortSailors1() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 0));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(0, 47, 120))),
          new Tuple(new ArrayList<>(Arrays.asList(0, 49, 176))),
          new Tuple(new ArrayList<>(Arrays.asList(0, 58, 191)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the sort statement for sailors tables - "multiple column"
   *
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testSortSailors2() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 1));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(0, 47, 120))),
          new Tuple(new ArrayList<>(Arrays.asList(0, 49, 176))),
          new Tuple(new ArrayList<>(Arrays.asList(0, 58, 191)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  /**
   * Test the select statement for sailors tables - multiple columns
   *
   * @throws ExecutionControl.NotImplementedException
   */
  @Test
  public void testSortSailors3() throws ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(index + 2));

    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);

    int expectedSize = 1000;

    Assertions.assertEquals(expectedSize, tuples.size(), "Unexpected number of rows.");

    // Check the first 3 tuple
    Tuple[] expectedFirstThreeTuples =
        new Tuple[] {
          new Tuple(new ArrayList<>(Arrays.asList(0, 47, 120))),
          new Tuple(new ArrayList<>(Arrays.asList(0, 49, 176))),
          new Tuple(new ArrayList<>(Arrays.asList(0, 58, 191)))
        };

    for (int i = 0; i < expectedFirstThreeTuples.length; i++) {
      Tuple expectedTuple = expectedFirstThreeTuples[i];
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }
}
