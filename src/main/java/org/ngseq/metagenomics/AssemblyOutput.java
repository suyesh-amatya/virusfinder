package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

public class AssemblyOutput {

    public static void parsing (String input) {
        SparkConf conf = new SparkConf().setAppName("MultipleSingleNodeAssemler");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> aggregateRDD = sc.textFile(input + "/aggregated_assembly_cdhit");

        JavaRDD<String> crdd = aggregateRDD.filter(f -> f.startsWith(">")).map(fasta -> {

            String[] fseq = fasta.trim().split("\n", 2);
            String id = fseq[0].split(" ")[0];
            System.out.println("there are fseq + " + fseq[0]);
            System.out.println("there are ids + " + id);


            //Give unique id for sequence
            String seq_id = id + "_" + UUID.randomUUID().toString();
            System.out.println("there are seq_id + " + seq_id);


            String seq = Arrays.toString(Arrays.copyOfRange(fseq, 1, fseq.length)).replace(", ", "").replace("[", "").replace("]", "");
            System.out.println("there are seq + " + seq);
            return ">" + seq_id + "\n" + seq;
        });

        crdd.saveAsTextFile(input + "/result");
        sc.stop();
    }
}
