package compiler;

import io_handler.BinaryHandler;
import builder.IndexBuilder;
import common.index.IndexInfo;
import builder.QueryPlanBuilder;
import common.tree.TreeNode;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
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
  private static final boolean outputToFiles = true;
  private static String outputDir;
  private static String inputDir;

  private static void evaluateSQL() {
    try {
      String str = Files.readString(Paths.get(inputDir + "/queries.sql"));
      Statements statements = CCJSqlParserUtil.parseStatements(str);
      QueryPlanBuilder queryPlanBuilder = new QueryPlanBuilder();

      if (outputToFiles) {
        for (File file : (new File(outputDir).listFiles())) {
          file.delete(); // clean output directory
        }
      }

      int counter = 1; // for numbering output files
      for (Statement statement : statements.getStatements()) {
        logger.info("Processing query: " + statement);

        try {
          Operator plan = queryPlanBuilder.buildPlan(statement);

          if (outputToFiles) {
            File outfile = new File(outputDir + "/query" + counter);
            outfile.createNewFile();
            long time;
            System.out.println(time = System.currentTimeMillis());
            // System.out.println(System.currentTimeMillis());
            plan.dump(new BinaryHandler(outfile));
            // System.out.println(System.currentTimeMillis());
            System.out.println(time - System.currentTimeMillis());
          } else {
            plan.dump(System.out);
          }
        } catch (Exception e) {
          logger.error(e.getMessage());
        }
        System.out.println("------------------------");
        ++counter;
      }
    } catch (Exception e) {
      for (StackTraceElement ste : e.getStackTrace()) {
        System.out.println(ste + "\n");
      }
      System.err.println("Exception occurred in interpreter");
      logger.error(e.getMessage());
    }
  }

  private static void buildIndex() {
    for (IndexInfo indexInfo : DBCatalog.getInstance().getAllIndexInfo()) {
      IndexBuilder ib = new IndexBuilder(indexInfo);
      List<TreeNode> nodes = ib.build();
      try {

        // Initialize file and file channel
        File indexFile =
            new File(
                inputDir + "/db/indexes/" + indexInfo.relationName + "." + indexInfo.attributeName);
        indexFile.createNewFile();
        FileOutputStream fileOutputStream = new FileOutputStream(indexFile);
        FileChannel fileChannel = fileOutputStream.getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(DBCatalog.getInstance().getBufferCapacity());

        // write header
        ib.writeHeader(byteBuffer);
        fileChannel.write(byteBuffer);

        // write nodes
        for (TreeNode node : nodes) {
          node.serialize(byteBuffer);
          fileChannel.write(byteBuffer);
        }

        fileChannel.close();
        fileOutputStream.close();
      } catch (Exception e) {
        for (StackTraceElement ste : e.getStackTrace()) {
          System.out.println(ste + "\n");
        }
        System.err.println("Exception occurred in interpreter" + e.getCause());
        logger.error(e.getMessage());
      }
    }
  }

  /**
   * Reads statements from queriesFile one at a time, builds query plan and evaluates, dumping
   * results to files or console as desired.
   *
   * <p>If dumping to files result of ith query is in file named query i, indexed stating at 1.
   */
  public static void main(String[] args) {
    DBCatalog.getInstance().setInterpreterConfig(args[0]);
    inputDir = DBCatalog.getInstance().getInputDir();
    outputDir = DBCatalog.getInstance().getOutputDir();

    if (DBCatalog.getInstance().getIsBuildIndex()) {
      buildIndex();
    }

    if (DBCatalog.getInstance().getIsEvaluateSQL()) {
      evaluateSQL();
    }
  }
}
