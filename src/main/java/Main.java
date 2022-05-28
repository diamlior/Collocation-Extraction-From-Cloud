import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String stopWords = "";
        try (Stream<String> lines = Files.lines(Paths.get("eng-stopwords.txt"))) {
            stopWords = lines.collect(Collectors.joining(System.lineSeparator()));
        } catch (Exception e){
            e.printStackTrace();
            System.err.println("Could not create stop words.");
        }
        conf.set("stop.words", stopWords);
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        Job job = Job.getInstance(conf, "Collocation-MapReduce1");
        job.setJarByClass(MapReducer1.class);
        job.setMapperClass(MapReducer1.TokenizerMapper.class);
        job.setMapOutputKeyClass(WordAndYear.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(MapReducer1.IntSumReducer.class);
        job.setReducerClass(MapReducer1.IntSumReducer.class);
        job.setOutputKeyClass(WordAndYear.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Configuration conf2 = new Configuration();
        conf2.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf2.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        Job job2 = Job.getInstance(conf2, "Collocation-MapReduce2");
        job2.setJarByClass(MapReducer2.class);
        job2.setMapperClass(MapReducer2.Mapper2.class);
        job2.setMapOutputKeyClass(WordAndCounter.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setNumReduceTasks(7);
        job2.setPartitionerClass(MapReducer2.DecadePartitioner2.class);
        job2.setReducerClass(MapReducer2.Reducer2.class);
        job2.setOutputKeyClass(WordAndCounter.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_final"));


        ControlledJob jobOneControl = new ControlledJob(job.getConfiguration());
        jobOneControl.setJob(job);

        ControlledJob jobTwoControl = new ControlledJob(job2.getConfiguration());
        jobTwoControl.setJob(job2);

        JobControl jobControl = new JobControl("job-control");
        jobControl.addJob(jobOneControl);
        jobControl.addJob(jobTwoControl);
        jobTwoControl.addDependingJob(jobOneControl); // this condition makes the job-2 wait until job-1 is done

        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();
        int code = 0;
        while (!jobControl.allFinished()) {
            code = jobControl.getFailedJobList().size() == 0 ? 0 : 1;
            Thread.sleep(1000);
        }
        for(ControlledJob j : jobControl.getFailedJobList())
            System.out.println(j.getMessage());
        System.exit(code);
    }
}
