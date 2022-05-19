package edu.indiana.d2i.compare;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.cassandra.CassandraManager;
import edu.indiana.d2i.ingest.util.Configuration;
import edu.indiana.d2i.ingest.util.METSParser.VolumeRecord;
import edu.indiana.d2i.ingest.util.METSParser.VolumeRecord.PageRecord;
import edu.indiana.d2i.ingest.util.Tools;

// VolumeComparison
// this class compares the page sequences of a volume if it exists in Cassandra, with the 
// page sequences in the zip file of the volume in the pairtree, e.g., if the volume in Cassandra
// has more pages a new zip file for the volume rsynced into the pairtree
public class VolumeComparison {
	private static Logger log = LogManager.getLogger(VolumeComparison.class);
	private PrintWriter pwEmptyZip;
	private PrintWriter pwNewZipFewerPages;
	private PrintWriter pwNewZipExtraPages;
	private PrintWriter pwNewZipSamePages;
	private CassandraManager cassandraManager;
	private String columnFamilyName;
	
	private long totalTimeForSelects = 0;

	// results of the updatePages method
	enum ComparePagesResult {
		SUCCESS,
		PAGE_CHECKSUM_MISMATCH_ERROR,
		EMPTY_ZIP_ERROR,
		METS_ZIP_MISMATCHED_PAGES_ERROR,
		VOLUME_ZIP_ERROR,
		CASSANDRA_READ_ERROR,
		OTHER;
	}
	
	public VolumeComparison() {
		cassandraManager = CassandraManager.getInstance();
		columnFamilyName = Configuration.getProperty(Constants.PK_VOLUME_TEXT_COLUMN_FAMILY);
		if (!cassandraManager.checkTableExist(columnFamilyName)) {
			System.out.println("Table " + columnFamilyName + "does not exist; creating table");
		}
		try {
			pwEmptyZip = new PrintWriter("volumesWithEmptyZip.txt");
			pwNewZipFewerPages = new PrintWriter("newZipFewerPages.txt");
			pwNewZipExtraPages = new PrintWriter("newZipExtraPages.txt");
			pwNewZipSamePages = new PrintWriter("newZipSamePages.txt");		
		} catch (FileNotFoundException e) {
			log.error("error creating printwriter for checksum verification", e.getMessage());
		}
	}

	public boolean compareNewZipWithExistingVolume(String volumeId) {
		if(volumeId == null || volumeId.equals("")) return false;
		
		String cleanId = Tools.cleanId(volumeId);
		String pairtreePath = Tools.getPairtreePath(volumeId);
		
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
		ComparePagesResult result = ComparePagesResult.OTHER;
		try {
			result = comparePages(volumeZipFile, volumeRecord);
			if (result == ComparePagesResult.SUCCESS) {
				return true;
			} else {
				return false;
			}
		} catch (FileNotFoundException e) {
			log.error("file not found" + e.getMessage());
			return false;
		}
	}

	private ComparePagesResult comparePages(File volumeZipFile, VolumeRecord volumeRecord) throws FileNotFoundException {
		String volumeId = volumeRecord.getVolumeID();

		// determine the page sequences of the volume if it already exists in the
		// database
		SimpleStatement selectPageSeqStmt
			= SimpleStatement.newInstance("SElECT sequence FROM " + columnFamilyName +
																		" WHERE volumeid = ?", volumeId);		
		Set<String> existingPageSequences = Collections.emptySet();
		try {
			long startTime = System.nanoTime();
			ResultSet result = cassandraManager.executeWithoutRetry(selectPageSeqStmt.setConsistencyLevel(ConsistencyLevel.ONE));
			long endTime = System.nanoTime();
			this.totalTimeForSelects += (endTime - startTime);
			
			List<Row> rows = result.all(); // rows is an empty List if the vol does
			                               // not exist in the database
			existingPageSequences = rows.stream()
				.map(row -> row.getString("sequence"))
				.collect(Collectors.toSet());
		} catch (NoNodeAvailableException nhae) {
			log.error("comparePages: Failed to get page sequences for volume " + volumeId,
								nhae);
			return ComparePagesResult.CASSANDRA_READ_ERROR;
		}
		
		if (existingPageSequences.isEmpty()) {
			log.info("Volume {} does not exist in Cassandra", volumeId);
			return ComparePagesResult.SUCCESS;
		}
		int i=0;
		try {
			ZipInputStream zis = new ZipInputStream(new FileInputStream(volumeZipFile));
			ZipEntry zipEntry = null;
			// keep track of the pages in the ZIP file, to compare later against
			// the pages in the METS file
			Set<String> pagesInZipFile = new HashSet<String>();
			// set of page sequences in the the zip file that is ingested, to compare
			// against existing page sequences, if the volume already exists in the 
			// database
			Set<String> newPageSequences = new HashSet<String>();

			while((zipEntry = zis.getNextEntry()) != null) {
				String entryName = zipEntry.getName();
				String entryFilename = extractEntryFilename(entryName);
				PageRecord pageRecord = volumeRecord.getPageRecordByFilename(entryFilename);
				if(pageRecord == null) {
					log.error("No PageRecord found by " + entryFilename + " in volume zip " + volumeZipFile.getAbsolutePath());
					continue;
				}
				if(entryFilename != null && !"".equals(entryFilename)) {
					// read page contents in bytes
/*					byte[] pageContents = readPagecontentsFromInputStream(zis);
					if(pageContents == null) {
						log.error("failed reading page contents for " + entryName + " of " + volumeId);
						continue;
					}
					
					//2. check against checksum of this page declared in METS
					String checksum = pageRecord.getChecksum();
					String checksumType = pageRecord.getChecksumType();
					try {
						String calculatedChecksum = Tools.calculateChecksum(pageContents, checksumType);
						if (!checksum.equals(calculatedChecksum)) {
							log.warn("Actual checksum and checksum from METS mismatch for entry " + entryName + " for volume: " + volumeId + ". Actual: " + calculatedChecksum
									+ " from METS: " + checksum);
							log.info("Recording actual checksum");
							// pageRecord.setChecksum(calculatedChecksum, checksumType);
							pwChecksumInfo.println(volumeId + "#" + entryFilename); pwChecksumInfo.flush();
							return ComparePagesResult.PAGE_CHECKSUM_MISMATCH_ERROR; // directly return false if mismatch happens
						} else {
							log.info("verified checksum for page " + entryFilename + " of " + volumeId);
						}
					} catch (NoSuchAlgorithmException e) {
						log.error("NoSuchAlgorithmException for checksum algorithm " + checksumType);
						log.error("Using checksum found in METS with a leap of faith");
					}
*/					
					// get 8-digit sequence for this page
					int order = pageRecord.getOrder();
					String sequence = generateSequence(order);
					pageRecord.setSequence(sequence);
										
					pagesInZipFile.add(entryFilename);
					newPageSequences.add(pageRecord.getSequence());
					i++;
				}
			} // end of while loop
			
			zis.close();				
			if (i == 0) {
				log.error("Cannot get entry from ZIP (zip file is probably empty) " + volumeZipFile.getAbsolutePath());
				if(!volumeRecord.getPageFilenameSet().isEmpty()) {
					pwEmptyZip.println(volumeId + " METS mismatch"); pwEmptyZip.flush();
				} else {
					pwEmptyZip.println(volumeId + " consistent with METS"); pwEmptyZip.flush();
				}
				return ComparePagesResult.EMPTY_ZIP_ERROR;
			}
			
			// check if the pages listed in the METS file match the pages found in the ZIP file
			if (!pagesInZipFile.equals(volumeRecord.getPageFilenameSet())) {
				log.error("Pages listed in METS file do not match pages in ZIP file: volumeId = {}", volumeId);
				return ComparePagesResult.METS_ZIP_MISMATCHED_PAGES_ERROR;
			}
			
			String msg = "First and last pages of " + volumeId;
			logFirstLast(msg + ", existing in Cassandra: {}, {}", existingPageSequences);
			logFirstLast(msg + ", new zip in pairtree: {}, {}", newPageSequences);
			
			// pageSequencesToDelete is the set difference (existingPageSequences -
			// newPageSequences)
			Set<String> pageSequencesToDelete = new HashSet(existingPageSequences);
			pageSequencesToDelete.removeAll(newPageSequences);
			newPageSequences.removeAll(existingPageSequences);
			if (!pageSequencesToDelete.isEmpty()) {
				pwNewZipFewerPages.println(volumeId);
				pwNewZipFewerPages.flush();
				log.info("Volume {} in Cassandra has the following extra pages compared to new zip: {}", volumeId, 
						pageSequencesToDelete.stream().collect(Collectors.joining(",")));
			}
			
			if (!newPageSequences.isEmpty()) {
				pwNewZipExtraPages.println(volumeId);
				pwNewZipExtraPages.flush();
				log.info("Volume {} in Cassandra does not have the following pages compared to new zip: {}", volumeId, 
						newPageSequences.stream().collect(Collectors.joining(",")));
			}
			
			if (pageSequencesToDelete.isEmpty() && newPageSequences.isEmpty()) {
				pwNewZipSamePages.println(volumeId);
				pwNewZipSamePages.flush();
				log.info("Volume {} in Cassandra has the same pages as the new zip: {}", volumeId, 
						newPageSequences.stream().collect(Collectors.joining(",")));				
			}
		} catch (IOException e) {
			log.error("IOException getting entry from ZIP " + volumeZipFile.getAbsolutePath(), e);
			return ComparePagesResult.VOLUME_ZIP_ERROR;
		}
		
		return ComparePagesResult.SUCCESS;
	}
	
	private void logFirstLast(String msg, Set<String> s) {
		List<String> ls = s.stream().sorted().collect(Collectors.toList());
		log.info(msg, ls.get(0), ls.get(ls.size() - 1));
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
    
    public long getTotalTimeForSelects() {
    	return this.totalTimeForSelects;
    }
    
    public void close() {
    	pwEmptyZip.flush();
    	pwEmptyZip.close();
    	pwNewZipFewerPages.flush();
    	pwNewZipFewerPages.close();
    	pwNewZipExtraPages.flush();
    	pwNewZipExtraPages.close();
    	pwNewZipSamePages.flush();
    	pwNewZipSamePages.close();
    }
}
