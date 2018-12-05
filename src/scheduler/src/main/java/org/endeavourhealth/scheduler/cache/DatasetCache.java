package org.endeavourhealth.scheduler.cache;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.scheduler.json.DatasetDefinition.DatasetConfig;
import org.endeavourhealth.scheduler.models.database.DataSetEntity;

import java.util.HashMap;
import java.util.Map;

public class DatasetCache {
    private static Map<Integer, DatasetConfig> datasetConfigMap = new HashMap<>();

    public static DatasetConfig getDatasetConfig(int datasetId) throws Exception {

        // Check if the config is already in the cache
        DatasetConfig config = datasetConfigMap.get(datasetId);

        if (config == null) {
            // get the config from the DB
            DataSetEntity dataset = DataSetEntity.getDatasetDefinition(datasetId);

            String definition = dataset.getDefinition();
            if (!StringUtils.isEmpty(definition)) {

                // Map config to the Java Class
                config = ObjectMapperPool.getInstance().readValue(definition, DatasetConfig.class);

                datasetConfigMap.put(datasetId, config);
            }
        }

        return config;
    }
}
