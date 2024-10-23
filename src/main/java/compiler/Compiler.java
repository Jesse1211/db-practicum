package compiler;

import common.BinaryHandler;
import common.DBCatalog;
import common.QueryPlanBuilder;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.apache.logging.log4j.*;
import physical_operator.Operator;

/**
 * Top level harness class; reads queries from an input file one at a time, processes them and sends
 * output to file or to System depending on flag.
 */
public class Compiler {
  private static final Logger logger = LogManager.getLogger();

  private static String outputDir;
  private static String inputDir;
  private static String tempDir;
  private static final boolean outputToFiles = true;

  /**
   * Reads statements from queriesFile one at a time, builds query plan and evaluates, dumping
   * results to files or console as desired.
   *
   * <p>If dumping to files result of ith query is in file named query i, indexed stating at 1.
   */
  public static void main(String[] args) {

    inputDir = args[0];
    outputDir = args[1];
    tempDir = args[2];
    DBCatalog.getInstance().setDataDirectory(inputDir + "/db");
    DBCatalog.getInstance().setPlanBuilderConfig(inputDir + "/plan_builder_config.txt");
    DBCatalog.getInstance().setTempDirectory(tempDir + "/");
    try {
      String str = Files.readString(Paths.get(inputDir + "/queries.sql"));
      Statements statements = CCJSqlParserUtil.parseStatements(str);
      QueryPlanBuilder queryPlanBuilder = new QueryPlanBuilder();

      if (outputToFiles) {
        for (File file : (new File(outputDir).listFiles())) file.delete(); // clean output directory
      }

      int counter = 1; // for numbering output files
      for (Statement statement : statements.getStatements()) {

        logger.info("Processing query: " + statement);

        try {
          Operator plan = queryPlanBuilder.buildPlan(statement);

          if (outputToFiles) {
            File outfile = new File(outputDir + "/query" + counter);
            outfile.createNewFile();
            System.currentTimeMillis();
            plan.dump(new BinaryHandler(outfile));
            System.currentTimeMillis();
          } else {
            plan.dump(System.out);
          }
        } catch (Exception e) {
          logger.error(e.getMessage());
        }
        System.err.println("------------------------");
        ++counter;
      }
    } catch (Exception e) {
      System.err.println("Exception occurred in interpreter");
      logger.error(e.getMessage());
    }
  }
}
