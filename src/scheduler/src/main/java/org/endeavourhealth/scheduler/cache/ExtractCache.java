package org.endeavourhealth.scheduler.cache;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;

import java.util.HashMap;
import java.util.Map;

public class ExtractCache {
    private static Map<Integer, ExtractConfig> extractConfigMap = new HashMap<>();
    private static Map<Integer, ExtractEntity> extractMap = new HashMap<>();

    public static ExtractConfig getExtractConfig(int extractId) throws Exception {

        // Check if the config is already in the cache
        ExtractConfig config = extractConfigMap.get(extractId);

        if (config == null) {
            // get the config from the DB
            ExtractEntity extract = getExtract(extractId);

            String definition = extract.getDefinition();
            if (!StringUtils.isEmpty(definition)) {

                // Map config to the Java Class
                config = ObjectMapperPool.getInstance().readValue(definition, ExtractConfig.class);

                extractConfigMap.put(extractId, config);
            }
        }

        return config;
    }

    public static ExtractEntity getExtract(int extractId) throws Exception {

        // Check if the config is already in the cache
        ExtractEntity extractEntity = extractMap.get(extractId);

        if (extractEntity == null) {
            // get the config from the DB
            extractEntity = ExtractEntity.getExtract(extractId);

            extractMap.put(extractId, extractEntity);
        }

        return extractEntity;
    }
}
