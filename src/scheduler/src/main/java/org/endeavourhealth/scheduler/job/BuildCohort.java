package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BuildCohort implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(BuildCohort.class);

    public void execute(JobExecutionContext jobExecutionContext) {

        List<ExtractEntity> extractsToProcess = (List<ExtractEntity>) jobExecutionContext.get("extractsToProcess");


    }
}
