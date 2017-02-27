package edu.indiana.d2i.ingest;

import java.io.File;

public class Constants {
	// property keys
	public static final String PK_VOLUME_ZIP_COLUMN_FAMILY = "VOLUME_ZIP_COLUMN_FAMILY";
	public static final String PK_VOLUME_TEXT_COLUMN_FAMILY = "VOLUME_TEXT_COLUMN_FAMILY";
	
	public static final String PK_MARC_JSON_FILES_FOLDER = "MARC-JSON-FILES-FOLDER";
	public static final String PK_MARC_JSON_FILES = "MARC-JSON-FILES";
	public static final String PK_MARC_COLFAMILY="MARC-COLUMN-FAMILY";
	public static final String PK_MARC_COLFAMILY_KEY="MARC-COLUMN-FAMILY-KEY";
	public static final String PK_MARC_COLUMN="MARC-COLUMN";
	public static final String PK_MARC_INGESTER_OUTFILE="MARC-INGESTER-OUTFILE";

	// default values
	public static final String DEFAULT_MARC_JSON_FILES_FOLDER = "/hathitrustmnt/marc-ingester/";
	public static final String DEFAULT_MARC_JSON_FILES = "meta_pd_open_access.json, meta_pd_google.json, meta_ic.json";
	public static final String DEFAULT_MARC_COLFAMILY="testmarc";
	public static final String DEFAULT_MARC_COLFAMILY_KEY="volumeid";
	public static final String DEFAULT_MARC_COLUMN="marc";
	public static final String DEFAULT_MARC_INGESTER_OUTFILE="marc-ingester-output.txt";
	
	// some constants to locate randomly distributed volume zip and mets files
	public static final String ROOT_PATH = "/hathitrustmnt";
	public static final String TO_PAIRTREE_PATH = "ingester-data/full_set";
	public static final String PAIRTREE_ROOT = "pairtree_root";
	public static final char SEPERATOR = File.separatorChar;
	public static final String VOLUME_ZIP_SUFFIX = ".zip";
	public static final String METS_XML_SUFFIX = ".mets.xml";
}
