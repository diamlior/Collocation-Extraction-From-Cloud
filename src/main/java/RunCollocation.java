import org.jets3t.service.security.AWSCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.util.ArrayList;
import java.util.List;

public class RunCollocation {
    public static void main(String[] args) {
        final String usage = "\n" +
                "Usage: " +
                "   <jar> <myClass> <keys> <logUri> <name>\n\n" +
                "Where:\n" +
                "   jar - A path to a JAR file run during the step. \n\n" +
                "   myClass - The name of the main class in the specified Java file. \n\n" +
                "   keys - The name of the Amazon EC2 key pair. \n\n" +
                "   logUri - The Amazon S3 bucket where the logs are located (for example,  s3://<BucketName>/logs/). \n\n" +
                "   name - The name of the job flow. \n\n" ;

//        if (args.length != 5) {
//            System.out.println(usage);
//            System.exit(1);
//        }

        String jar = "s3://collocation-ds/Collocation-Extraction-From-Cloud.jar" ; // TODO INSERT JAR
        String myClass = "Main" ;
        String logUri = "s3://collocation-ds/logger/" ;
        String name = "collocation-extraction" ;
        Region region = Region.US_EAST_1;
        EmrClient emrClient = EmrClient.builder()
                .region(region)
                .credentialsProvider(ProfileCredentialsProvider.create("default"))
                .build();

        String jobFlowId = createAppCluster(emrClient, jar, myClass, logUri, name);
        System.out.println("The job flow id is " +jobFlowId);
        emrClient.close();
    }
    // snippet-start:[emr.java2._create_cluster.main]
    public static String createAppCluster( EmrClient emrClient,
                                           String jar,
                                           String myClass,
                                           String logUri,
                                           String name) {

        try {
            HadoopJarStepConfig jarStepConfig = HadoopJarStepConfig.builder()
                    .jar(jar)
                    .args("eng")
                    .build();

            StepConfig enabledebugging = StepConfig.builder()
                    .name("Enable debugging")
                    .actionOnFailure("TERMINATE_JOB_FLOW")
                    .hadoopJarStep(jarStepConfig)
                    .build();

            JobFlowInstancesConfig instancesConfig = JobFlowInstancesConfig.builder()
                    .instanceCount(8)
                    .keepJobFlowAliveWhenNoSteps(false)
                    .masterInstanceType("m4.xlarge")
                    .slaveInstanceType("m4.xlarge")
                    .build();


            RunJobFlowRequest jobFlowRequest = RunJobFlowRequest.builder()
                    .name(name)
                    .releaseLabel("emr-5.20.0")
                    .steps(enabledebugging)
                    .logUri(logUri)
                    .serviceRole("EMR_DefaultRole")
                    .jobFlowRole("EMR_EC2_DefaultRole")
                    .instances(instancesConfig)

                    .build();

            RunJobFlowResponse response = emrClient.runJobFlow(jobFlowRequest);
            return response.jobFlowId();

        } catch(EmrException e){
            System.err.println(e.getMessage());
            System.exit(1);
        }

        return "";
    }
}
