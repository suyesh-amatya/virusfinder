package org.ngseq.metagenomics;
import com.github.lindenb.jbwa.jni.BwaIndex;
import com.github.lindenb.jbwa.jni.BwaMem;
import com.github.lindenb.jbwa.jni.ShortRead;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class EstimateReads {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("EstimateReads");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Options options = new Options();
        Option pathOpt = new Option("in", true, "Path to fastq file in hdfs.");    //gmOpt.setRequired(true);
        Option refOpt = new Option("ref", true, "Path to fasta reference in local FS. (index must be available on every node under the same path)");
        Option fqoutOpt = new Option("out", true, "");
        options.addOption(pathOpt);
        options.addOption(refOpt);
        options.addOption(fqoutOpt);

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
        String ref = (cmd.hasOption("ref") == true) ? cmd.getOptionValue("ref") : null;
        String outDir = (cmd.hasOption("out") == true) ? cmd.getOptionValue("out") : null;

        FileSystem fs = FileSystem.get(new Configuration());

        FileStatus[] fastq = fs.listStatus(new Path(input));

        for (FileStatus file : fastq) {
            String file_path = file.getPath().toUri().getRawPath();
            List<String> items = Arrays.asList(file_path.split("\\s*/\\s*"));
            String name = items.get(items.size() - 1).split("\\.")[0];
            Broadcast<String> bs = sc.broadcast(name);

            JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(file_path, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());


            JavaRDD<String> alignmentRDD = fastqRDD.mapPartitions(split -> {
                try {
                    System.load("/lib/native/libbwajni.so");
                } catch (UnsatisfiedLinkError e) {
                    System.err.println("Native code library failed to load.\n" + e);
                    System.exit(1);
                }

                BwaIndex index = new BwaIndex(new File(ref));
                BwaMem mem = new BwaMem(index);


                List<ShortRead> L1 = new ArrayList<ShortRead>();
                List<ShortRead> L2 = new ArrayList<ShortRead>();


                while (split.hasNext()) {
                    Tuple2<Text, SequencedFragment> next = split.next();
                    String key = next._1.toString();

                    SequencedFragment sf = new SequencedFragment();
                    sf.setQuality(new Text(next._2.getQuality().toString()));
                    sf.setSequence(new Text(next._2.getSequence().toString()));

                    if (split.hasNext()) {

                        Tuple2<Text, SequencedFragment> next2 = split.next();
                        String key2 = next2._1.toString();

                        SequencedFragment sf2 = new SequencedFragment();
                        sf2.setQuality(new Text(next2._2.getQuality().toString()));
                        sf2.setSequence(new Text(next2._2.getSequence().toString()));

                        if (key.equalsIgnoreCase(key2)) {
                            L1.add(new ShortRead(key, sf.getSequence().toString().getBytes(), sf.getQuality().toString().getBytes()));
                            L2.add(new ShortRead(key2, sf2.getSequence().toString().getBytes(), sf2.getQuality().toString().getBytes()));
                        } else
                            split.next();
                    }
                }
                String[] aligns = mem.align(L1, L2);


                if (aligns != null) {


                    ArrayList<String> filtered = new ArrayList<String>();

                    Arrays.asList(aligns).forEach(aln -> {
                        String[] fields = aln.split("\\t");
                        int flag = Integer.parseInt(fields[1]);


                        if (flag != 77 && flag != 141) {
                            String read_name = fields[0];
                            String contig = fields[2];
                            String bases = fields[9];
                            String quality = fields[10];

                            // filtered.add(flag+ "\t" + contig + "\t" + name + "\t" + bases + "\t" + quality);
                            filtered.add(contig);

                        }

                    });
                    return filtered.iterator();
                } else
                    return new ArrayList<String>().iterator(); //NULL ALIGNMENTS


            });

            JavaPairRDD<String, Integer> counts = alignmentRDD.flatMap(x -> Arrays.asList(x).iterator())
                    .mapToPair(x -> new Tuple2<>(x, 1))
                    .reduceByKey((x, y) -> x + y);

            JavaRDD<String> countRdd = counts.map(x -> {
                String contig = x._1;
                String number = x._2.toString();
                String line = contig + "\t" + number+ "\t" + name;
                return line;

            });
            countRdd.coalesce(1).saveAsTextFile(outDir + "/" + name);
        }
        FileStatus[] dr = fs.listStatus(new Path(outDir));
        for (FileStatus dir : dr) {

            FileStatus[] files = fs.listStatus(dir.getPath());
            for (int i = 0; i < files.length; i++) {
                String fn = files[i].getPath().getName();

                if (!fn.equalsIgnoreCase("_SUCCESS")) {
                    String folder = dir.getPath().toUri().getRawPath();
                    String fileName = folder.substring(folder.lastIndexOf("/") + 1) + ".txt";

                    Path srcPath = new Path(files[i].getPath().toUri().getRawPath());
                    String newPath = dir.getPath().getParent().toUri().getRawPath() + "/" + fileName;
                    Path dstPath = new Path(newPath);

                    FileUtil.copy(fs, srcPath, fs, dstPath, true, new Configuration());
                    fs.delete(new Path(dir.getPath().toUri().getRawPath()));
                }

            }
        }
        sc.stop();

    }


}
