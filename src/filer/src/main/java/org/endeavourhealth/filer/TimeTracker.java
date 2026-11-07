package org.endeavourhealth.filer;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * quick tool for tracking what the application is doing over time
 */
public class TimeTracker {
    private static final Logger LOG = LoggerFactory.getLogger(TimeTracker.class);

    private static String prefix;
    private static Map<String, Long> hmResults = new HashMap<>();
    private static String lastAction;
    private static long lastActionStart;

    public static void start(String appName) {
        prefix = appName;
    }

    public static void logAction(String action) {
        long end = System.currentTimeMillis();
        finishLastAction(end);
        lastAction = prefix + "." + action;
        lastActionStart = end;
    }

    private static void finishLastAction(long end) {
        if (Strings.isNullOrEmpty(lastAction)) {
            return;
        }

        long msDur = end - lastActionStart;
        String[] toks = lastAction.split(Pattern.quote(".")); //use the Pattern.quote fn to make it NOT use regex
        for (int i=0; i<toks.length; i++) {

            List<String> subToks = new ArrayList<>();
            for (int j=0; j<=i; j++) {
                String tok = toks[j];
                subToks.add(tok);
            }
            String subTokStr = String.join(".", subToks);

            long subDur = msDur;
            Long l = hmResults.get(subTokStr);
            if (l != null) {
                //LOG.debug("Action " + subTokStr + " = " + subDur + " total = " + (subDur + l.longValue()));
                subDur += l.longValue();
            } else {
                //LOG.debug("Action " + subTokStr + " = " + subDur);
            }

            hmResults.put(subTokStr, new Long(subDur));
        }
    }

    public static void stop() {
        long end = System.currentTimeMillis();
        finishLastAction(end);
        logResults();
    }

    private static void logResults() {
        List<String> keys = new ArrayList<>(hmResults.keySet());
        keys.sort((o1, o2) -> {
            return o1.compareToIgnoreCase(o2);
        });

        if (keys.isEmpty()) {
            LOG.debug("No time usage data recorded");
            return;
        }
        String firstKey = keys.get(0);
        Long totalTime = hmResults.get(firstKey);

        LOG.debug("Time Usage Breakdown");
        for (String key: keys) {
            Long dur = hmResults.get(key);


            String prefix = key;
            String suffix = "" + dur + " ms";
            while (prefix.length() + suffix.length() < 100) {
                prefix += ".";
            }

            int per = (int)((100D * dur.doubleValue()) / totalTime.doubleValue());
            suffix += "    " + per + " %";

            LOG.debug(prefix + suffix);
        }
    }
}
