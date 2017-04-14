package edu.indiana.d2i.ingest.solr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import edu.indiana.d2i.ingest.util.Configuration;

public enum AcronymToFullNameMapper {
	language (new File(Configuration.getProperty("METADATA_TRANSLATION_MAP_DIR"), Configuration.getProperty("LANGUAGE_MAP_FILE"))),
	country (new File(Configuration.getProperty("METADATA_TRANSLATION_MAP_DIR"), Configuration.getProperty("COUNTRY_MAP_FILE")));
	
	private Properties properties ;
	private File mappingFile;
	AcronymToFullNameMapper (File _mappingFile) {
		this.properties = new Properties();
		this.mappingFile = _mappingFile;
		try {
			properties.load(new FileInputStream(this.mappingFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Set<String> getFullNames(Set<String> acronym) {
		Set<String> set = new HashSet<String>();
		acronym.stream().forEach(acron -> {
			String value = properties.getProperty(acron);
			if(value != null) {
				set.add(value);
			}
		});
		return set;
	}
}
