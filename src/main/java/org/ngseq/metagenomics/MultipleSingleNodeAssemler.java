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
import org.seqdoop.hadoop_bam.SequencedFragment;

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
        SparkConf conf = new SparkConf().setAppName("MultipleSingleNodeAssemler");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        Options options = new Options();

        Option splitOpt = new Option("in", true, "");
        Option cOpt = new Option("t", true, "Threads");
        Option kOpt = new Option("m", true, "fraction of memory to be used per process");
        Option ouOpt = new Option("out", true, "");

        options.addOption(new Option("localdir", true, "Absolute path to local temp dir ( YARN must have write permissions if YARN used)"));
        options.addOption(new Option("debug", "saves error log"));
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


        FileSystem fs = FileSystem.get(new Configuration());
        //fs.mkdirs(fs, new Path(outDir), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

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

        Dataset<Row> sortedPairDF = pairDF.sort("key");


        String path = "hdfs:///Projects/indexes/Resources/ViraOutput/" + tempName;


        dfToFasta(sortedPairDF).coalesce(100).saveAsTextFile(path);



        fs.copyToLocalFile(true, new Path(path), new Path(localdir));


        String pathToLocalFasta = localdir + "/" + tempName;

        String cat = "cat " + pathToLocalFasta + "/part-* > "+pathToLocalFasta+"/fasta.fa";
        executeBashCommand(cat);
        String rm = "rm "+pathToLocalFasta+"/part-*";
        executeBashCommand(rm);



        String config_file = "echo \"max_rd_len=150\n" +
                "[LIB]\n" +
                "#average insert size\n" +
                "avg_ins=300\n" +
                "#if sequence needs to be reversed\n" +
                "reverse_seq=0\n" +
                "#in which part(s) the reads are used\n" +
                "asm_flags=3\n" +
                "#use only first 100 bps of each read\n" +
                "rd_len_cutoff=100\n" +
                "#in which order the reads are used while scaffolding\n" +
                "rank=1\n" +
                "# cutoff of pair number for a reliable connection (at least 3 for short insert size)\n" +
                "pair_num_cutoff=3\n" +
                "#minimum aligned length to contigs for a reliable read location (at least 32 for short insert size)\n" +
                "map_len=32\n" +
                "#a pair of fastq file, read 1 file should always be followed by read 2 file\n" +
                "p="+pathToLocalFasta+"/fasta.fa\n \" >"+pathToLocalFasta+"/soap.config.txt";

        executeBashCommand(config_file);


        String mkdirs = "mkdir "+pathToLocalFasta+"/soap";
        executeBashCommand(mkdirs);

       // String Soapdenovo = "SOAPdenovo-63mer  all -s "+pathToLocalFasta+"/soap.config.txt -K 31 -R -o "+pathToLocalFasta+"/soap/31 1 >"+pathToLocalFasta+"/soap/ass.log 2 > "+pathToLocalFasta+"/soap/ass.err" + " & "
         //       + "SOAPdenovo-Trans-31mer  all -s "+pathToLocalFasta+"/soap.config.txt -K 31 -R -o "+pathToLocalFasta+"/soaptrans/31 1 >"+pathToLocalFasta+"/soaptrans/ass.log 2 > "+pathToLocalFasta+"/soaptrans/ass.err" +" & "
           //     + "/mnt/hdfs/2/idba/bin/idba --pre_correction -r "+pathToLocalFasta+"/fasta.fa -o "+pathToLocalFasta+"/idba";




        ArrayList<Integer> kmers = new ArrayList<Integer>(){{add(31);add(27);add(25);}};



        for (int kmer : kmers) {
            String mkdir = "mkdir "+pathToLocalFasta+"/soap/"+kmer;
            executeBashCommand(mkdir);
            String Soapdenovo = "SOAPdenovo-63mer  all -s " + pathToLocalFasta + "/soap.config.txt -K "+kmer+" -R -o " + pathToLocalFasta + "/soap/"+kmer+"/"+kmer+" 1 >" + pathToLocalFasta + "/soap/ass.log 2 > " + pathToLocalFasta + "/soap/ass.err";
            executeBashCommand(Soapdenovo);

            String movingFile = "mv " + pathToLocalFasta + "/soap/"+kmer+"/"+kmer+".scafSeq " + pathToLocalFasta + "/soap/";
            executeBashCommand(movingFile);
            String dl = "rm -rf " +pathToLocalFasta + "/soap/"+kmer;
            executeBashCommand(dl);

            int last = kmers.get(kmers.size() - 1);
            if (last == kmer) {
                System.out.println("last element " + last);
                String catAssembled = "cat " + pathToLocalFasta + "/soap/*.scafSeq > " + pathToLocalFasta + "/soap/aggregated_soap.fasta";
                executeBashCommand(catAssembled);

            }



        }


        sc.stop();

    }





    private static JavaRDD<String> dfToFasta(Dataset<Row> df) {

        return df.toJavaRDD().map(row -> {

            String name = row.getAs("key");

            if (row.getAs("read").equals(1)) {
                name = name + "/1";
            } else {
                name = name + "/2";
            }

            String sequence = row.getAs("sequence").toString();
            //TODO: check values
            String output = ">" + name + "\n" + sequence;

            return output;

        });

    }



    private static void executeBashCommand(String command) {
        try {

            System.out.println("bash command  " + command);
            ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);
            Process process = pb.start();
            BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String e;
            ArrayList<String> out = new ArrayList<String>();
            while ((e = err.readLine()) != null) {
                System.out.println(e);
                out.add(e);
            }
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



}
