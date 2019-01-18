package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import static org.apache.spark.sql.functions.count;

/**
 * Created by zurbzh on 2018-10-16.
 */
public class SingleNodeMultipleAssemler {


    private static String tablename = "records";


    public static void  main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("SingleNodeMultipleAssemler");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Options options = new Options();

        Option in = new Option("in", true, "");
        Option out = new Option("out", true, "");
        Option local = new Option("localdir", true, "");
        Option cases = new Option("cases", true, "");


        options.addOption(in);
        options.addOption(out);
        options.addOption(local);
        options.addOption(cases);

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
        String patients = cmd.getOptionValue("cases");



        FileSystem fs = FileSystem.get(new Configuration());
        fs.mkdirs(fs, new Path(outDir), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

        Path patinentFile = new Path(patients);
        FSDataInputStream inputStream = fs.open(patinentFile);
        //Classical input stream usage
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        ArrayList<String> lines = new ArrayList<String>();
        while ((line = br.readLine()) != null) {
            String caseId = line.split("\t")[0];
            lines.add(caseId);
        }
        br.close();



        inputStream.close();
        //fs.close();

        String tempName = String.valueOf((new Date()).getTime());

        String hdfsTempname = "hdfs:///Projects/TCGA/Resources/" + tempName;


        FileStatus[] files = fs.listStatus(new Path(inputPath));

        for (FileStatus file : files) {
            String fl = file.getPath().toUri().getRawPath();
            List<String> items = Arrays.asList(fl.split("\\s*/\\s*"));
            String name = items.get(items.size() - 1).split("\\.")[0];
            if (lines.contains(name))
            {

                Path srcPath = new Path(file.getPath().toUri().getRawPath());
                Path dstPath = new Path( hdfsTempname+ "/" + name + ".fq");
                FileUtil.copy(fs, srcPath, fs, dstPath, false, new Configuration());

            }
        }




        fs.mkdirs(fs,new Path(outDir),new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));


        fs.copyToLocalFile(false, new Path(hdfsTempname), new Path(localdir));


        String pathToLocalFasta = localdir + "/" + tempName;

        String cat = "cat " + pathToLocalFasta + "/*.fq > "+pathToLocalFasta+"/fasta.fa";
        executeBashCommand(cat);
        String rm = "rm "+pathToLocalFasta+"/*.fq";
        executeBashCommand(rm);



        String config_file = "echo \"max_rd_len=50\n" +
                "[LIB]\n" +
                "#average insert size\n" +
                "avg_ins=200\n" +
                "#if sequence needs to be reversed\n" +
                "reverse_seq=0\n" +
                "#in which part(s) the reads are used\n" +
                "asm_flags=3\n" +
                "#use only first 100 bps of each read\n" +
                "rd_len_cutoff=50\n" +
                "#in which order the reads are used while scaffolding\n" +
                "rank=1\n" +
                "# cutoff of pair number for a reliable connection (at least 3 for short insert size)\n" +
                "pair_num_cutoff=3\n" +
                "#minimum aligned length to contigs for a reliable read location (at least 32 for short insert size)\n" +
                "map_len=32\n" +
                "#a pair of fastq file, read 1 file should always be followed by read 2 file\n" +
                "p="+pathToLocalFasta+"/fasta.fa\n \" >"+pathToLocalFasta+"/soap.config.txt";

        executeBashCommand(config_file);


        String mkdirs = "mkdir "+pathToLocalFasta+"/soap" + " & " + "mkdir "+pathToLocalFasta+"/idba"+" & " + "mkdir "+pathToLocalFasta+"/soaptrans";
        executeBashCommand(mkdirs);

       // String Soapdenovo = "SOAPdenovo-63mer  all -s "+pathToLocalFasta+"/soap.config.txt -K 31 -R -o "+pathToLocalFasta+"/soap/31 1 >"+pathToLocalFasta+"/soap/ass.log 2 > "+pathToLocalFasta+"/soap/ass.err" + " & "
         //       + "SOAPdenovo-Trans-31mer  all -s "+pathToLocalFasta+"/soap.config.txt -K 31 -R -o "+pathToLocalFasta+"/soaptrans/31 1 >"+pathToLocalFasta+"/soaptrans/ass.log 2 > "+pathToLocalFasta+"/soaptrans/ass.err" +" & "
           //     + "/mnt/hdfs/2/idba/bin/idba --pre_correction -r "+pathToLocalFasta+"/fasta.fa -o "+pathToLocalFasta+"/idba";




        ArrayList<Integer> kmers = new ArrayList<Integer>(){{add(15);add(19);add(21);}};



        for (int kmer : kmers) {


            // run SOAPdenovo-63mer
            String mkdir = "mkdir "+pathToLocalFasta+"/soap/"+kmer;
            executeBashCommand(mkdir);
            String Soapdenovo = "SOAPdenovo-63mer  all -s " + pathToLocalFasta + "/soap.config.txt -K "+kmer+" -R -o " + pathToLocalFasta + "/soap/"+kmer+"/"+kmer+" 1 >" + pathToLocalFasta + "/soap/ass.log 2 > " + pathToLocalFasta + "/soap/ass.err";
            executeBashCommand(Soapdenovo);
            String movingFile = "mv " + pathToLocalFasta + "/soap/"+kmer+"/"+kmer+".scafSeq " + pathToLocalFasta + "/soap/";
            executeBashCommand(movingFile);
            String dl = "rm -rf " +pathToLocalFasta + "/soap/"+kmer;
            executeBashCommand(dl);

            // run SOAPdenovo-Trans-31mer
            String mkdirtrans = "mkdir "+pathToLocalFasta+"/soaptrans/"+kmer;
            executeBashCommand(mkdirtrans);
            String SoapdenovoTrans = "SOAPdenovo-Trans-31mer  all -s "+pathToLocalFasta+"/soap.config.txt -K "+kmer+"  -R -o "+pathToLocalFasta+"/soaptrans/"+kmer+"/"+kmer+" 1 >"+pathToLocalFasta+"/soaptrans/ass.log 2 > "+pathToLocalFasta+"/soaptrans/ass.err";
            executeBashCommand(SoapdenovoTrans);
            String movingFiletrans = "mv " + pathToLocalFasta + "/soaptrans/"+kmer+"/"+kmer+".scafSeq " + pathToLocalFasta + "/soaptrans/";
            executeBashCommand(movingFiletrans);
            String dltrans = "rm -rf " +pathToLocalFasta + "/soaptrans/"+kmer;
            executeBashCommand(dltrans);




            int last = kmers.get(kmers.size() - 1);
            if (last == kmer) {

                String catAssembled = "cat " + pathToLocalFasta + "/soap/*.scafSeq > " + pathToLocalFasta + "/soap/aggregated_soap.fasta";
                executeBashCommand(catAssembled);

                //String catAssembledtrans = "cat " + pathToLocalFasta + "/soaptrans/*.scafSeq > " + pathToLocalFasta + "/soaptrans/aggregated_soap.fasta";
                //executeBashCommand(catAssembledtrans);

                // run idba assemblre
                String idba = "idba --pre_correction -r "+pathToLocalFasta+"/fasta.fa -o "+pathToLocalFasta+"/idba";
                executeBashCommand(idba);

            }

        } // for loop for kmers


        File idba = new File(pathToLocalFasta+"/idba/scaffold.fa");

        if (idba.exists()) {

            String getAllcontigs = "cat " + pathToLocalFasta + "/soap/aggregated_soap.fasta " + pathToLocalFasta + "/idba/scaffold.fa > " + pathToLocalFasta + "/final_contigs.fa";
            executeBashCommand(getAllcontigs);

        } else {
            String getAllcontigs = "cat " + pathToLocalFasta + "/soap/aggregated_soap.fasta " + pathToLocalFasta + "/idba/contig.fa > " + pathToLocalFasta + "/final_contigs.fa";
            executeBashCommand(getAllcontigs);
        }

        String cdhit = "cd-hit-est -i " + pathToLocalFasta +"/final_contigs.fa -o " +pathToLocalFasta +"/aggregated_assembly_cdhit -d 100 -T 0 -r 1 -g 1 -c 0.98 -G 0 -aS 0.95 -G 0 -M 0";
        executeBashCommand(cdhit);


        String final_local_path = pathToLocalFasta +"/aggregated_assembly_cdhit";
        fs.copyFromLocalFile(new Path(final_local_path), new Path(outDir));


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

            System.out.println("bash command - " + command);
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
