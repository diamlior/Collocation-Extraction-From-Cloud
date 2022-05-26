import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
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
        Job job2 = Job.getInstance(conf2, "word count");
        job2.setJarByClass(MapReducer2.class);
        job2.setMapperClass(MapReducer2.Mapper2.class);
        job2.setMapOutputKeyClass(WordAndCounter.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setCombinerClass(MapReducer2.Reducer2.class);
        job2.setReducerClass(MapReducer2.Reducer2.class);
        job2.setOutputKeyClass(WordAndCounter.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path("output_final"));


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
        System.exit(code);
    }
}
