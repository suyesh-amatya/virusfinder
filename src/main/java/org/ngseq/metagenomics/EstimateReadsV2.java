package org.ngseq.metagenomics;
import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.api.java.function.Function;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EstimateReadsV2 {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("EstimateReadsV2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        Options options = new Options();
        Option pathOpt = new Option("in", true, "Path to fastq file in hdfs.");    //gmOpt.setRequired(true);
        Option pathVir = new Option("pathVir", true, "Path to fastq file in hdfs.");    //gmOpt.setRequired(true);
        Option outOpt = new Option("out", true, "");
        options.addOption(pathOpt);
        options.addOption(outOpt);
        options.addOption(pathVir);


        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("spark-submit <spark specific args>", options, true);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
        }
        String input = cmd.getOptionValue("in");
        String blastViruses = cmd.getOptionValue("pathVir");
        String outDir = (cmd.hasOption("out") == true) ? cmd.getOptionValue("out") : null;

        Dataset countedReads = registerCountedReads(input, sc, sqlContext);
        countedReads.registerTempTable("CountedReads");
        countedReads.show();

        Dataset viruses = registerBlastViruses(blastViruses, sc, sqlContext);
        viruses.registerTempTable("Viruses");

        viruses.show();

        String sql = "SELECT CountedReads.contig, CountedReads.count, Viruses.gi, Viruses.length, CountedReads.case, Viruses.family FROM CountedReads " +
                "LEFT JOIN Viruses ON CountedReads.contig = Viruses.contig " +
                "ORDER BY CountedReads.case";

        Dataset combined = sqlContext.sql(sql);
        combined.show();

        JavaRDD<String> combinedRDD = dfToTabDelimited(combined);
        JavaRDD<String> filteredRDD = combinedRDD.filter(x -> !x.contains("null"));
        filteredRDD.saveAsTextFile(outDir);

    }

    private static JavaRDD<String> dfToTabDelimited(Dataset<Row> df) {
        return df.toJavaRDD().map(row -> {
            //qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore

            String output = row.getAs("contig") + "\t" + row.getAs("count") + "\t" + row.getAs("length")
                    + "\t" + row.getAs("family")+ "\t" + row.getAs("case");

            return output;
        });
    }

    private static Dataset<Row> registerCountedReads (String input, JavaSparkContext sc, SQLContext sqlContext) {
        JavaRDD<String> contigs = sc.textFile(input);

// The schema is encoded in a string
        String schemaString = "contig count case";

// Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName: schemaString.split(" ")) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);

// Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = contigs.map(
                new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        String[] fields = record.split("\t");
                        return RowFactory.create(fields[0], fields[1].trim(),fields[2]);
                    }
                });

// Apply the schema to the RDD.
        Dataset peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);
        return peopleDataFrame;

    }

    private static Dataset<Row> registerBlastViruses (String input, JavaSparkContext sc, SQLContext sqlContext) {
        JavaRDD<String> contigs = sc.textFile(input);

// The schema is encoded in a string
        String schemaString = "contig gi acc identity coverage length e-value organism family subfamily subfamilyType";

// Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName: schemaString.split(" ")) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);

// Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = contigs.map(
                new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        String[] fields = record.split("\\|");
                        return RowFactory.create(fields[0], fields[1],fields[2],fields[3],fields[4],fields[5],fields[6],fields[7],fields[8],fields[9],fields[10]);
                    }
                });

// Apply the schema to the RDD.
        Dataset peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);
        return peopleDataFrame;

    }

}