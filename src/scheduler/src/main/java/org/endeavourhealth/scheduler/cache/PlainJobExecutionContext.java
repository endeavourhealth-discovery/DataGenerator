package org.endeavourhealth.scheduler.cache;

import org.quartz.*;

import java.util.Date;
import java.util.HashMap;

public class PlainJobExecutionContext implements JobExecutionContext {

    private HashMap map = new HashMap();

    @Override
    public void put(Object key, Object value) {
        map.put(key, value);
    }

    @Override
    public Object get(Object key) {
        return map.get(key);
    }

    // Everything  below this comment is unused.

    @Override
    public Scheduler getScheduler() {
        return null;
    }

    @Override
    public Trigger getTrigger() {
        return null;
    }

    @Override
    public Calendar getCalendar() {
        return null;
    }

    @Override
    public boolean isRecovering() {
        return false;
    }

    @Override
    public TriggerKey getRecoveringTriggerKey() throws IllegalStateException {
        return null;
    }

    @Override
    public int getRefireCount() {
        return 0;
    }

    @Override
    public JobDataMap getMergedJobDataMap() {
        return null;
    }

    @Override
    public JobDetail getJobDetail() {
        return null;
    }

    @Override
    public Job getJobInstance() {
        return null;
    }

    @Override
    public Date getFireTime() {
        return null;
    }

    @Override
    public Date getScheduledFireTime() {
        return null;
    }

    @Override
    public Date getPreviousFireTime() {
        return null;
    }

    @Override
    public Date getNextFireTime() {
        return null;
    }

    @Override
    public String getFireInstanceId() {
        return null;
    }

    @Override
    public Object getResult() {
        return null;
    }

    @Override
    public void setResult(Object o) {

    }

    @Override
    public long getJobRunTime() {
        return 0;
    }
}
