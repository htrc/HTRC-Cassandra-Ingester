package edu.indiana.d2i.ingest;

import java.io.File;

public class Constants {
	// property keys
	public static final String PK_VOLUME_ZIP_COLUMN_FAMILY = "VOLUME_ZIP_COLUMN_FAMILY";
	public static final String PK_VOLUME_TEXT_COLUMN_FAMILY = "VOLUME_TEXT_COLUMN_FAMILY";
    // some constants to locate randomly distributed volume zip and mets files
    public static final String ROOT_PATH = "/hathitrustmnt";
	public static final String TO_PAIRTREE_PATH = "ingester-data/full_set";
	public static final String PAIRTREE_ROOT = "pairtree_root";
	public static final char SEPERATOR = File.separatorChar;
	public static final String VOLUME_ZIP_SUFFIX = ".zip";
	public static final String METS_XML_SUFFIX = ".mets.xml";
}
