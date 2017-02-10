package edu.indiana.d2i.ingest.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.exceptions.WriteFailureException;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.Ingester;
import edu.indiana.d2i.ingest.util.Configuration;
import edu.indiana.d2i.ingest.util.METSParser.VolumeRecord;
import edu.indiana.d2i.ingest.util.METSParser.VolumeRecord.PageRecord;
import edu.indiana.d2i.ingest.util.Tools;

public class CassandraPageTextIngester extends Ingester{
	private static Logger log = LogManager.getLogger(CassandraPageTextIngester.class);
	private CassandraManager cassandraManager;
	private String columnFamilyName;
	
	public CassandraPageTextIngester() {
		cassandraManager = CassandraManager.getInstance();
		columnFamilyName = Configuration.getProperty(Constants.PK_VOLUME_TEXT_COLUMN_FAMILY);
		if(! cassandraManager.checkTableExist(columnFamilyName)) {
			String createTableStr = "CREATE TABLE " + columnFamilyName + " ("
		    		+ "volumeID text, "
					+ "accessLevel int static, "
		    		//+ "language text static, "
					//+ "mets text static,"
		    		+ "volumeByteCount bigint static, "
					+ "volumeCharacterCount int static, "
		    		+ "sequence text, "
		    		+ "byteCount bigint, "
		    		+ "characterCount int, "
		    		+ "contents text, "
		    		+ "checksum text, "
		    		+ "checksumType text, "
		    		+ "pageNumberLabel text, "
		    		+ "PRIMARY KEY (volumeID, sequence))";
			cassandraManager.execute(createTableStr);
		}
	}
	
	public void ingest(List<String> volumes) {
		for(String id : volumes) {
			ingestOne(id);
		}
		
	}

	public boolean ingestOne(String volumeId) {
		String cleanId = Tools.cleanId(volumeId);
		String pairtreePath = Tools.getPairtreePath(cleanId);
		
		String cleanIdPart = cleanId.split("\\.", 2)[1];
		String zipFileName = cleanIdPart  + Constants.VOLUME_ZIP_SUFFIX; // e.g.: ark+=13960=t02z18p54.zip
		String metsFileName = cleanIdPart + Constants.METS_XML_SUFFIX; // e.g.: ark+=13960=t02z18p54.mets.xml
		/*
		 *  get the zip file and mets file for this volume id based on relative path(leafPath) and zipFileName or metsFileName
		 *  e.g.: /hathitrustmnt/silvermaple/ingester-data/full_set/loc/pairtree_root/ar/k+/=1/39/60/=t/8h/d8/d9/4r/ark+=13960=t8hd8d94r/ark+=13960=t8hd8d94r.zip
		 *  /hathitrustmnt/silvermaple/ingester-data/full_set/loc/pairtree_root/ar/k+/=1/39/60/=t/8h/d8/d9/4r/ark+=13960=t8hd8d94r/ark+=13960=t8hd8d94r.mets.xml
		 */
		File volumeZipFile = Tools.getFileFromPairtree(pairtreePath, zipFileName);
		File volumeMetsFile = Tools.getFileFromPairtree(pairtreePath, metsFileName);
		if(volumeZipFile == null || volumeMetsFile == null || !volumeZipFile.exists() || !volumeMetsFile.exists()) {
			log.error("zip file or mets file does not exist for " + volumeId);
			return false;
		}
		
		VolumeRecord volumeRecord = Tools.getVolumeRecord(volumeId, volumeMetsFile);
		boolean volumeAdded = false;
		try {
			volumeAdded = updatePages(volumeZipFile, volumeRecord);
		} catch (FileNotFoundException e) {
			log.error("file not found" + e.getMessage());
		}
		if(volumeAdded) {
			log.info("text ingested successfully " + volumeId);
		} else {
			log.info("text ingest failed " + volumeId);
		}
		return volumeAdded;
	}

	private boolean updatePages(File volumeZipFile, VolumeRecord volumeRecord) throws FileNotFoundException {
		String volumeId = volumeRecord.getVolumeID();
		boolean volumeAdded = false;
		BatchStatement batchStmt = new BatchStatement(); // a batch to insert all pages of this volume
		
		long volumeByteCount = 0;
		long volumeCharacterCount = 0;
		int i=0;
		try {
			Insert firstPageInsert = null;
			ZipInputStream zis = new ZipInputStream(new FileInputStream(volumeZipFile));
			ZipEntry zipEntry = null;
			while((zipEntry = zis.getNextEntry()) != null) {
				String entryName = zipEntry.getName();
				String entryFilename = extractEntryFilename(entryName);
				PageRecord pageRecord = volumeRecord.getPageRecordByFilename(entryFilename);
				if(pageRecord == null) {
					log.error("No PageRecord found by " + entryFilename + " in volume zip " + volumeZipFile.getAbsolutePath());
					continue;
				}
				if(entryFilename != null && !"".equals(entryFilename)) {
					//1. read page contents in bytes
					byte[] pageContents = readPagecontentsFromInputStream(zis);
					if(pageContents == null) {
						log.error("failed reading page contents for " + entryName + " of " + volumeId);
						continue;
					}
					//2. verify byte count of this page
					if(pageContents.length != pageRecord.getByteCount() ) {
						log.warn("Actual byte count and byte count from METS mismatch for entry " + entryName + " for volume " + volumeId + ". Actual: " + pageContents.length + " from METS: " + pageRecord.getByteCount());
						log.info("Recording actual byte count");
						pageRecord.setByteCount(pageContents.length);
						volumeByteCount += pageContents.length;
					} else {
						volumeByteCount += pageRecord.getByteCount();
						log.info("verified page content for page " + entryFilename + " of " + volumeId);
					}
					//3. check against checksum of this page declared in METS
					String checksum = pageRecord.getChecksum();
					String checksumType = pageRecord.getChecksumType();
					try {
						String calculatedChecksum = Tools.calculateChecksum(pageContents, checksumType);
						if (!checksum.equals(calculatedChecksum)) {
							log.warn("Actual checksum and checksum from METS mismatch for entry " + entryName + " for volume: " + volumeId + ". Actual: " + calculatedChecksum
									+ " from METS: " + checksum);
							log.info("Recording actual checksum");
							pageRecord.setChecksum(calculatedChecksum, checksumType);
						} else {
							log.info("verified checksum for page " + entryFilename + " of " + volumeId);
						}
					} catch (NoSuchAlgorithmException e) {
                        log.error("NoSuchAlgorithmException for checksum algorithm " + checksumType);
                        log.error("Using checksum found in METS with a leap of faith");
                    }
					
					//4. get 8-digit sequence for this page
					int order = pageRecord.getOrder();
					String sequence = generateSequence(order);
					pageRecord.setSequence(sequence);
					
					//5.  convert to string and count character count -- NOTE: some pages are not encoded in utf-8, but there is no charset indicator, so assume utf-8 for all for now
                    String pageContentsString = new String(pageContents, "utf-8");
                    pageRecord.setCharacterCount(pageContentsString.length());
                    volumeCharacterCount += pageContentsString.length();
                    
                    //6. add page content into batch
                    Insert insertStmt = QueryBuilder
							.insertInto(columnFamilyName);
					insertStmt
							.value("volumeID", volumeId)
							.value("sequence", pageRecord.getSequence())
							.value("byteCount", pageRecord.getByteCount())
							.value("characterCount",
									pageRecord.getCharacterCount())
							.value("contents", pageContentsString)
							.value("checksum", pageRecord.getChecksum())
							.value("checksumType", pageRecord.getChecksumType())
							.value("pageNumberLabel", pageRecord.getLabel());
					batchStmt.add(insertStmt);
					if(i == 0) {
						firstPageInsert = insertStmt;
					}
					i++;
				}
			}
			zis.close();
			//7. add static columns/fields into the first page
			if (firstPageInsert != null) {
				firstPageInsert.value("accessLevel", 1) // will decide access level based on rights database
						.value("volumeByteCount", volumeByteCount)
						.value("volumeCharacterCount", volumeCharacterCount);
			}
			//8. then push the volume into cassandra
			cassandraManager.execute(batchStmt);
		} catch (IOException e) {
			log.error("IOException getting entry from ZIP " + volumeZipFile.getAbsolutePath(), e);
			return false;
		} catch (WriteFailureException e) {
			log.error("write failure for " + volumeId + ": " + e.getMessage());
			return false;
		}
		
		volumeAdded = true;
		return volumeAdded;
	}

	private byte[] readPagecontentsFromInputStream(ZipInputStream zis) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		int read = -1;
		byte[] buffer = new byte[32767];
		try {
			while((read = zis.read(buffer)) > 0) {
				bos.write(buffer, 0, read);
			}
		} catch (IOException e) {
			log.error("error reading zip stream" + e.getMessage());
		}
		try {
			bos.close();
		} catch (IOException e) {
			log.error("IOException while attempting to close ByteArrayOutputStream()" + e.getMessage());
		}
		return bos.toByteArray();
	}

	/**
     * Method to extract the filename from a ZipEntry name
     * @param entryName name of a ZipEntry
     * @return extracted filename
     */
    protected String extractEntryFilename(String entryName) {
        int lastIndex = entryName.lastIndexOf('/');
        return entryName.substring(lastIndex + 1);
    }
    
    static final int SEQUENCE_LENGTH = 8;
    
    /**
     * Method to generate a fixed-length zero-padded page sequence number
     * @param order the ordering of a page
     * @return a fixed-length zero-padded page sequence number based on the ordering
     */
    protected String generateSequence(int order) {
        String orderString = Integer.toString(order);
        StringBuilder sequenceBuilder = new StringBuilder();
        
        int digitCount = orderString.length();
        for (int i = digitCount; i < SEQUENCE_LENGTH; i++) {
            sequenceBuilder.append('0');
        }
        sequenceBuilder.append(orderString);
        return sequenceBuilder.toString();
    }
}
