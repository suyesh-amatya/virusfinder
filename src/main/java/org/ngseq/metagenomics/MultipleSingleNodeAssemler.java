package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;

import static org.apache.spark.sql.functions.count;

/**
 * Created by zurbzh on 2018-10-16.
 */
public class MultipleSingleNodeAssemler {


    private static String tablename = "records";


    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Assemble");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Options options = new Options();

        Option splitOpt = new Option("in", true, "");
        Option cOpt = new Option("t", true, "Threads");
        Option kOpt = new Option("m", true, "fraction of memory to be used per process");
        Option ouOpt = new Option("out", true, "");

        options.addOption(new Option("localdir", true, "Absolute path to local temp dir ( YARN must have write permissions if YARN used)"));
        options.addOption(new Option("merge", "Merge output"));
        options.addOption(new Option("subdirs", "Read from subdirectories"));
        options.addOption(new Option("debug", "saves error log"));
        options.addOption(new Option("bin", true, "Path to megahit binary, defaults calls 'megahit'"));
        options.addOption(new Option("single", "Single reads option, default is interleaved paired-end"));
        options.addOption(splitOpt);
        options.addOption(cOpt);
        options.addOption(kOpt);
        options.addOption(ouOpt);

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("spark-submit <spark specific args>", options, true);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            System.exit(1);
        }
        String inputPath = (cmd.hasOption("in") == true) ? cmd.getOptionValue("in") : null;
        String outDir = (cmd.hasOption("out") == true) ? cmd.getOptionValue("out") : null;
        String localdir = cmd.getOptionValue("localdir");
        boolean subdirs = cmd.hasOption("subdirs");
        boolean debug = cmd.hasOption("debug");
        String readstype = (cmd.hasOption("single") == true) ? "-r" : "--12";
        String bin = (cmd.hasOption("bin") == true) ? cmd.getOptionValue("bin") : "megahit";

        int t = (cmd.hasOption("t") == true) ? Integer.valueOf(cmd.getOptionValue("t")) : 1;
        double m = (cmd.hasOption("m") == true) ? Double.valueOf(cmd.getOptionValue("m")) : 0.9;
        boolean mergeout = cmd.hasOption("merge");

        FileSystem fs = FileSystem.get(new Configuration());
        fs.mkdirs(fs, new Path(outDir), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

        JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(inputPath, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());


        JavaRDD<MyRead> rdd = fastqRDD.map(record -> {
            MyRead read = new MyRead();
            read.setKey(record._1.toString().split("/")[0]);
            read.setRead(Integer.parseInt(record._1.toString().split("/")[1]));
            read.setSequence(record._2.getSequence().toString());
            read.setQuality(record._2.getQuality().toString());
            return read;
        });

        Dataset df = sqlContext.createDataFrame(rdd, MyRead.class);
        df.registerTempTable(tablename);

        fs.mkdirs(fs,new Path(outDir),new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));
        String tempName = String.valueOf((new Date()).getTime());

        // find pair ends
        Dataset pairEndKeys = df.groupBy("key").agg(count("*").as("count")).where("count > 1");

        Dataset<Row> pairDF = pairEndKeys.join(df, pairEndKeys.col("key").equalTo(df.col("key"))).drop(pairEndKeys.col("key"));

        Dataset<Row> forward = pairDF.filter(pairDF.col("read").equalTo(1)).sort("key");
        Dataset<Row> reverse = pairDF.filter(pairDF.col("read").equalTo(2)).sort("key");


        String path = "hdfs:///Projects/indexes/Resources/ViraOutput/" + tempName + "/forward";


        dfToFastq(forward).coalesce(100).saveAsNewAPIHadoopFile(path, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
        dfToFastq(reverse).coalesce(100).saveAsNewAPIHadoopFile("hdfs:///Projects/indexes/Resources/ViraOutput/" + tempName + "/reverse", Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());


        String pathtoforward = "hdfs:///Projects/indexes/Resources/ViraOutput/" + tempName + "/forward/part-*";
        String pathtoreverse = "hdfs:///Projects/indexes/Resources/ViraOutput/" + tempName + "/reverse/part-*";

        String ass_cmd = "/srv/hops/hadoop/bin/hdfs dfs -text " + pathtoforward + " > "+localdir+"/"+tempName+"/forward.fq";
        String ass_cmd1 = "/srv/hops/hadoop/bin/hdfs dfs -text " + pathtoreverse + " > "+localdir+"/"+tempName+"/reverse.fq";


        System.out.println("forward copy " + ass_cmd);
        System.out.println("reverse copy " + ass_cmd1);


        ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", ass_cmd);
        pb.start();

        ProcessBuilder pb1 = new ProcessBuilder("/bin/sh", "-c", ass_cmd1);
        pb1.start();



    }



    private static JavaPairRDD<Text, SequencedFragment> dfToFastq(Dataset<Row> df) {

        return df.toJavaRDD().mapToPair(row -> {



            String name = row.getAs("key");


            //TODO: check values
            Text t = new Text(name);
            SequencedFragment sf = new SequencedFragment();
            sf.setSequence(new Text(row.getAs("sequence").toString()));
            sf.setQuality(new Text(row.getAs("quality").toString()));

            return new Tuple2<Text, SequencedFragment>(t, sf);
        });

    }



}
