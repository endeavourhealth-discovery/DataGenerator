package org.endeavourhealth.scheduler.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenerateData implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(GenerateData.class);

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Generate Data");

        //TODO Run the data extractor Stored Proc to move the data into new temporary tables

        System.out.println("End of Generate Data");
    }
}
