package org.endeavourhealth.scheduler.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildCohort implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(BuildCohort.class);

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Build Cohort");

        //TODO Call Darren's code for the rules regarding Cohort Generation

        System.out.println("End of Build Cohort");
    }
}
