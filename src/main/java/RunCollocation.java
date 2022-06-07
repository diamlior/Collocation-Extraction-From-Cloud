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

        String jar = "s3://collocation-ds/Collocation-Extraction-From-Cloud.jar" ; // TODO INSERT JAR
        String name = "collocation-extraction" ;
        String lang = "heb"; // Fill in heb / eng
        String logUri = "s3://collocation-ds/logger/" ;
        Region region = Region.US_EAST_1;
        EmrClient emrClient = EmrClient.builder()
                .region(region)
                .credentialsProvider(ProfileCredentialsProvider.create("default"))
                .build();

        String jobFlowId = createAppCluster(emrClient, jar, lang, logUri, name);
        System.out.println("The job flow id is " +jobFlowId);
        emrClient.close();
    }
    // snippet-start:[emr.java2._create_cluster.main]
    public static String createAppCluster( EmrClient emrClient,
                                           String jar,
                                           String lang,
                                           String logUri,
                                           String name) {

        try {
            HadoopJarStepConfig jarStepConfig = HadoopJarStepConfig.builder()
                    .jar(jar)
                    .args(lang)
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
