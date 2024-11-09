import common.BinaryHandler;
import common.DBCatalog;
import common.QueryPlanBuilder;
import common.Tuple;
import common.TupleReader;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import physical_operator.Operator;

public class P1UnitTests {

  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;
  private static Statements statements;
  private static ClassLoader classLoader;

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException, URISyntaxException {
    classLoader = P1UnitTests.class.getClassLoader();
    URI path = Objects.requireNonNull(classLoader.getResource("samples")).toURI();
    Path resourcePath = Paths.get(path);

    DBCatalog.getInstance()
        .setInterpreterConfig(resourcePath.resolve("interpreter_config_file.txt").toString());
    URI queriesFile =
        Objects.requireNonNull(classLoader.getResource("samples/input/queries.sql")).toURI();

    statements = CCJSqlParserUtil.parseStatements(Files.readString(Paths.get(queriesFile)));
    queryPlanBuilder = new QueryPlanBuilder();
    statementList = statements.getStatements();
  }

  @Test
  public void testQuery01() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(0));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query1")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery02() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(1));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query2")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery03() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(2));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query3")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery04() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(3));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query4")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery05() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(4));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query5")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery06() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(5));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query6")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery07() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(6));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query7")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery08() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(7));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query8")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();
    Map<Tuple, Integer> frequencyMap = new HashMap<>();

    for (Tuple tuple : expectedTuples) {
      frequencyMap.put(tuple, frequencyMap.getOrDefault(tuple, 0) + 1);
    }

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      int count = frequencyMap.getOrDefault(expectedTuple, 0);
      Assertions.assertTrue(count > 0, "Unexpected tuple at index " + i);
      frequencyMap.put(expectedTuple, count - 1);
    }
  }

  @Test
  public void testQuery09() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(8));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query9")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();
    Map<Tuple, Integer> frequencyMap = new HashMap<>();

    for (Tuple tuple : expectedTuples) {
      frequencyMap.put(tuple, frequencyMap.getOrDefault(tuple, 0) + 1);
    }

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      int count = frequencyMap.getOrDefault(expectedTuple, 0);
      Assertions.assertTrue(count > 0, "Unexpected tuple at index " + i);
      frequencyMap.put(expectedTuple, count - 1);
    }
  }

  @Test
  public void testQuery10() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(9));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query10")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();
    Map<Tuple, Integer> frequencyMap = new HashMap<>();

    for (Tuple tuple : expectedTuples) {
      frequencyMap.put(tuple, frequencyMap.getOrDefault(tuple, 0) + 1);
    }

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      int count = frequencyMap.getOrDefault(expectedTuple, 0);
      Assertions.assertTrue(count > 0, "Unexpected tuple at index " + i);
      frequencyMap.put(expectedTuple, count - 1);
    }
  }

  @Test
  public void testQuery11() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(10));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query11")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    Set<Tuple> expectedSet = new HashSet<>(expectedTuples);
    Set<Tuple> actualSet = new HashSet<>(tuples);

    Assertions.assertEquals(expectedSet, actualSet, "Unexpected tuples.");
  }

  @Test
  public void testQuery12() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(11));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query12")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();
    Map<Tuple, Integer> frequencyMap = new HashMap<>();

    for (Tuple tuple : expectedTuples) {
      frequencyMap.put(tuple, frequencyMap.getOrDefault(tuple, 0) + 1);
    }

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      int count = frequencyMap.getOrDefault(expectedTuple, 0);
      Assertions.assertTrue(count > 0, "Unexpected tuple at index " + i);
      frequencyMap.put(expectedTuple, count - 1);
    }
  }

  @Test
  public void testQuery13() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(12));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query13")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery14() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(13));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query14")).toURI();

    TupleReader tupleReader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = tupleReader.readAllTuples();

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  @Test
  public void testQuery15() throws URISyntaxException, ExecutionControl.NotImplementedException {
    Operator plan = queryPlanBuilder.buildPlan(statementList.get(14));
    URI expectedOutputFile =
        Objects.requireNonNull(classLoader.getResource("samples/expected_output/query15")).toURI();

    TupleReader reader = new BinaryHandler(new File(expectedOutputFile));
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    List<Tuple> expectedTuples = reader.readAllTuples();

    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");

    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }
}
