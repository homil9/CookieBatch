package com.cookie.agent.util;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

/**
 * 지정된 properties 파일로부터 Configuration 생성해서 제공
 */
public class ConfigurationManager {

    private static final Logger logger = Logger.getLogger(ConfigurationManager.class);
    private static Configuration configuration;
    
    static {
        try {
            if (configuration == null) {
                Parameters params = new Parameters();
                
                FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                        new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                        .configure(params.properties()
                        .setFileName("properties/agent.properties"));

                // Get a Configuration from builder
                configuration = builder.getConfiguration();
            }
        } catch (Exception e) {
            logger.error(ExceptionUtils.getStackTrace(e));
        }
    }
    
    /**
     * @return Configuration
     */
    public static Configuration getConfiguration() {
        return configuration;
    }
}
