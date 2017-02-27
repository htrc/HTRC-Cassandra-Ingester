package edu.indiana.d2i.ingest.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Configuration {

	private static Properties properties = new Properties();
	static {
		try {
			properties.load(new FileInputStream(new File("conf/conf.properties")));
		} catch (FileNotFoundException e) {
			System.out.println("configuration file is not found");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("ioexception when loading configuration file");
			e.printStackTrace();
		}
	}
	
	public static String getProperty(String key) {
		return properties.getProperty(key);
	}
	
	public static String getProperty(String key, String defaultValue) {
		return properties.getProperty(key, defaultValue);
	}
}
