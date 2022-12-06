package com.rombachuk.mongoreactivetest;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;

public class Configuration {

    Map<String, Map<String, String>> map;
    
    public Map<String, Map<String, String>> getMap() {
        return map;
    }

    public void setMap(Map<String, Map<String, String>> map) {
        this.map = map;
    }


    /**
     * @param filename
     * @throws FileNotFoundException
     * @throws IOException
     * @throws ConfigurationException
     */
    public Configuration (String filename) throws FileNotFoundException, IOException, ConfigurationException {

    final HierarchicalINIConfiguration iniConfiguration = new HierarchicalINIConfiguration();

    try (FileReader fileReader = new FileReader(filename)) {
            iniConfiguration.load(fileReader);
        } 
    this.map = new HashMap<>();

    for (String section : iniConfiguration.getSections()) {
        Map<String, String> subSectionMap = new HashMap<>();
        SubnodeConfiguration confSection = iniConfiguration.getSection(section);
        Iterator<String> keyIterator = confSection.getKeys();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            String value = confSection.getProperty(key).toString();
            subSectionMap.put(key, value);
        }
        this.map.put(section, subSectionMap);
    }

    }
    
}
