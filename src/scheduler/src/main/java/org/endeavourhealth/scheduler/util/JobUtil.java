package org.endeavourhealth.scheduler.util;

import org.quartz.JobExecutionContext;
import org.quartz.SchedulerException;

import java.util.concurrent.ThreadLocalRandom;
import java.util.List;

public class JobUtil {

    public static boolean isJobRunning(JobExecutionContext jobExecutionContext, String[] jobNames, String groupName)
            throws SchedulerException, InterruptedException {
        List<JobExecutionContext> currentJobs = jobExecutionContext.getScheduler().getCurrentlyExecutingJobs();

        for (JobExecutionContext jobCtx : currentJobs) {
            String thisJobName = jobCtx.getJobDetail().getKey().getName();
            String thisGroupName = jobCtx.getJobDetail().getKey().getGroup();
            for (String jobName : jobNames) {
                if (jobName.equalsIgnoreCase(thisJobName) && groupName.equalsIgnoreCase(thisGroupName)
                        && !jobCtx.getFireTime().equals(jobExecutionContext.getFireTime())) {
                    long randomNum = ThreadLocalRandom.current().nextInt(3, 5);
                    Thread.sleep(randomNum * 1000);
                    return true;
                }
            }
        }
        return false;
    }
}
