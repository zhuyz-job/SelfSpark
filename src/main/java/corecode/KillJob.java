package corecode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.cli.ApplicationCLI;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KillJob {
    public static void main(String[] args) throws Exception {
        String jobName="";
        if (args.length >0){
            jobName=args[0];
            getJobIdAndKill(jobName);
        }else {
            System.err.println("请输入job名称, 来杀死job.....");
        }
    }

    private static void getJobIdAndKill(String jobName) throws IOException, YarnException {
        ApplicationCLI cli = new ApplicationCLI();
        cli.setSysOutPrintStream(System.out);
        cli.setSysErrPrintStream(System.err);

        Set<String> appTypes = new HashSet<String>();
        EnumSet<YarnApplicationState> appStates = EnumSet
                .noneOf(YarnApplicationState.class);
        appStates.add(YarnApplicationState.RUNNING);
        appStates.add(YarnApplicationState.ACCEPTED);
        appStates.add(YarnApplicationState.SUBMITTED);

        //yarn客户端： 获取job信息
        YarnClient client = YarnClient.createYarnClient();
        client.init(new Configuration());
        client.start();
        List<ApplicationReport> appsReport = client.getApplications(appTypes, appStates);
        for (ApplicationReport rep: appsReport){
            String appName = rep.getName();
            ApplicationId appId = rep.getApplicationId();
            System.out.println("jobName=> "+appName+",  jobId=> "+appId.toString());
            if (jobName.equalsIgnoreCase(appName)){
                killApplication(appId.toString(), client);
            }
        }
    }

    public static void killApplication(String applicationId, YarnClient client) throws YarnException,
            IOException {
        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
        ApplicationReport  appReport = null;
        try {
            appReport = client.getApplicationReport(appId);
        } catch (ApplicationNotFoundException e) {
            System.out.println("Application with id '" + applicationId + "' doesn't exist in RM.");
            throw e;
        }
        if (appReport.getYarnApplicationState() == YarnApplicationState.FINISHED
                || appReport.getYarnApplicationState() == YarnApplicationState.KILLED
                || appReport.getYarnApplicationState() == YarnApplicationState.FAILED) {
            System.out.println("Application " + applicationId + " has already finished ");
        } else {
            System.out.println("Killing application " + applicationId);
            client.killApplication(appId);
        }
    }
}

