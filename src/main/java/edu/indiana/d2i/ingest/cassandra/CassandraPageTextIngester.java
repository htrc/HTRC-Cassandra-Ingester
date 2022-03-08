package edu.indiana.d2i.ingest.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;

//import com.datastax.driver.core.BatchStatement;
//import com.datastax.driver.core.ConsistencyLevel;
//import com.datastax.driver.core.exceptions.OperationTimedOutException;
//import com.datastax.driver.core.exceptions.WriteFailureException;
//import com.datastax.driver.core.exceptions.WriteTimeoutException;
//import com.datastax.driver.core.querybuilder.Insert;
//import com.datastax.driver.core.querybuilder.QueryBuilder;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.Ingester;
import edu.indiana.d2i.ingest.util.Configuration;
import edu.indiana.d2i.ingest.util.METSParser.VolumeRecord;
import edu.indiana.d2i.ingest.util.METSParser.VolumeRecord.PageRecord;
import edu.indiana.d2i.ingest.util.Tools;

public class CassandraPageTextIngester extends Ingester{
	private static Logger log = LogManager.getLogger(CassandraPageTextIngester.class);
	private PrintWriter pwChecksumInfo;
	private PrintWriter pwEmptyZip;
	private CassandraManager cassandraManager;
	private String columnFamilyName;
//	private Updater accessLevelUpdater;
	
	/*public CassandraPageTextIngester(Updater accessLevelUpdater) {
		this();
	//	this.accessLevelUpdater = accessLevelUpdater;
	}*/
	
	// results of the updatePages method
	enum UpdatePagesResult {
		SUCCESS,
		PAGE_CHECKSUM_MISMATCH_ERROR,
		EMPTY_ZIP_ERROR,
		METS_ZIP_MISMATCHED_PAGES_ERROR,
		VOLUME_ZIP_ERROR,
		CASSANSDRA_WRITE_ERROR,
		OTHER;
	}
	
	public CassandraPageTextIngester() {
		cassandraManager = CassandraManager.getInstance();
		columnFamilyName = Configuration.getProperty(Constants.PK_VOLUME_TEXT_COLUMN_FAMILY);
		if(! cassandraManager.checkTableExist(columnFamilyName)) {
			System.out.println("Table " + columnFamilyName + "does not exist; creating table");
			String createTableStr = "CREATE TABLE " + columnFamilyName + " ("
		    		+ "volumeID text, "
//					+ "idSource text STATIC, "  // mostly HT
//					+ "persistentId text STATIC, "       //reserved
//					+ "accessLevel int STATIC, "
		    		//+ "language text static, "
					+ "structMetadata text STATIC,"
		    		+ "structMetadataType text STATIC, "  // mostly METS
					+ "semanticMetadata text STATIC, "
					+ "semanticMetadataType text STATIC,"  // mostly MARC
					+ "lastModifiedTime timestamp STATIC,"
				    + "cksumValidationTime timestamp STATIC,"
//				    + "volumezip blob STATIC,"
		    		+ "volumeByteCount bigint STATIC, "
					+ "volumeCharacterCount int STATIC, "
		    		+ "sequence text, "
		    		+ "characterCount int, "
		    		+ "contents text, "
		    	//	+ "checksum text, "
		    	//	+ "checksumType text, "
		    		+ "pageNumberLabel text, "
		    		+ "PRIMARY KEY (volumeID, sequence))";
			cassandraManager.execute(createTableStr);
		}
		try {
			pwChecksumInfo = new PrintWriter("failedChecksumVolIds.txt");
			pwEmptyZip = new PrintWriter("volumesWithEmptyZip.txt");
		} catch (FileNotFoundException e) {
			log.error("error creating printwriter for checksum verification", e.getMessage());
		}
	}
	
	/*public void ingest(List<String> volumes) {
		for(String id : volumes) {
			ingestOne(id);
		}
		
	}*/

	public boolean ingestOne(String volumeId) {
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
		UpdatePagesResult result = UpdatePagesResult.OTHER;
		try {
			int maxAttempts = 3;
			while(maxAttempts > 0) {
				result = updatePages(volumeZipFile, volumeRecord);
				if (result == UpdatePagesResult.CASSANSDRA_WRITE_ERROR) {
					// retry the ingestion only if there has been an error while trying to write to Cassandra
					maxAttempts --;
					Thread.sleep(5000);
				} else {
					// if the ingestion is a success, or if the error is something other than a write error, then do not retry the ingestion
					break;
				}
			}
			
		} catch (FileNotFoundException e) {
			log.error("file not found" + e.getMessage());
		} catch (InterruptedException e) {
			log.error("ingest trhead interrupted" + e.getMessage());
		}
		if (result == UpdatePagesResult.SUCCESS) {
			log.info("text ingested successfully " + volumeId);
		/*	boolean accessLevelUpdated = accessLevelUpdater.update(volumeId);
			if(accessLevelUpdated) {
				log.info("access level updated successfully " + volumeId);
			} else {
				log.error("access level update failed " + volumeId);
			}*/
			return true;
		} else {
			log.info("text ingest failed " + volumeId);
			return false;
		}
		// return volumeAdded;
	}

	private UpdatePagesResult updatePages(File volumeZipFile, VolumeRecord volumeRecord) throws FileNotFoundException {
		String volumeId = volumeRecord.getVolumeID();
		// boolean volumeAdded = false;
//		BatchStatement batchStmt = new BatchStatement(); // a batch to insert all pages of this volume
		BatchStatementBuilder batchStmtBuilder = BatchStatement.builder(BatchType.LOGGED);
		
		long volumeByteCount = 0;
		int volumeCharacterCount = 0; // originally, long
		int i=0;
		try {
//			Insert firstPageInsert = null;
			BoundStatement firstPageInsert = null;
			ZipInputStream zis = new ZipInputStream(new FileInputStream(volumeZipFile));
			ZipEntry zipEntry = null;
			// keep track of the pages in the ZIP file, to compare later against the pages in the METS file
			Set<String> pagesInZipFile = new HashSet<String>();

			// insert statement to insert a single page and page-related data
			Insert insert = QueryBuilder.insertInto(columnFamilyName)
				.value("volumeid", QueryBuilder.bindMarker())
				.value("sequence", QueryBuilder.bindMarker())
				.value("byteCount", QueryBuilder.bindMarker())
				.value("characterCount", QueryBuilder.bindMarker())
				.value("contents", QueryBuilder.bindMarker())
				.value("pageNumberLabel", QueryBuilder.bindMarker());
			PreparedStatement insertStmt = cassandraManager.prepare(insert.build());

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
							return UpdatePagesResult.PAGE_CHECKSUM_MISMATCH_ERROR; // directly return false if mismatch happens
						} else {
							log.info("verified checksum for page " + entryFilename + " of " + volumeId);
						}
					} catch (NoSuchAlgorithmException e) {
						log.error("NoSuchAlgorithmException for checksum algorithm " + checksumType);
						log.error("Using checksum found in METS with a leap of faith");
					}
					
					//3. verify byte count of this page
					if(pageContents.length != pageRecord.getByteCount() ) {
						log.warn("Actual byte count and byte count from METS mismatch for entry " + entryName + " for volume " + volumeId + ". Actual: " + pageContents.length + " from METS: " + pageRecord.getByteCount());
						log.info("Recording actual byte count");
						pageRecord.setByteCount(pageContents.length);
						volumeByteCount += pageContents.length;
					} else {
						volumeByteCount += pageRecord.getByteCount();
						log.info("verified page content for page " + entryFilename + " of " + volumeId);
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
/*                    Insert insertStmt = QueryBuilder.insertInto(columnFamilyName)
                    		.value("volumeID", QueryBuilder.bindMarker())
                    		.value("sequence", QueryBuilder.bindMarker())
                    		.value("byteCount", QueryBuilder.bindMarker())
                    		.value("characterCount", QueryBuilder.bindMarker())
                    		.value("contents", QueryBuilder.bindMarker())
                    		.value("pageNumberLabel", QueryBuilder.bindMarker());
*/  
					//6. add page content into batch
					BoundStatement boundInsertStmt =
						insertStmt.bind(volumeId,
														pageRecord.getSequence(),
														pageRecord.getByteCount(),
														pageRecord.getCharacterCount(),
														pageContentsString,
														pageRecord.getLabel());
					
					pagesInZipFile.add(entryFilename);	
					batchStmtBuilder.addStatement(boundInsertStmt);
					if(i == 0) {
						firstPageInsert = boundInsertStmt;
					}
					i++;
				}
			} // end of while loop
			
			zis.close();
			//7. add static columns/fields 
			if (firstPageInsert != null) {
				// ByteBuffer zipBinaryContent = getByteBuffer(volumeZipFile);
/*				firstPageInsert.value("volumeByteCount", volumeByteCount)
						.value("volumeCharacterCount", volumeCharacterCount)
						.value("structMetadata", volumeRecord.getMETSContents())
						.value("structMetadataType", "METS")
				     // .value("volumezip", zipBinaryContent)
						.value("lastModifiedTime", new Date())
						.value("cksumValidationTime", new Date());
 					 //	.value("idSource", "Hathitrust");
*/				
				// simple statements with named parameters are currently not supported in batch statements; but the following should work
				// in a later version
/*				BuildableQuery insertStaticCols = QueryBuilder.insertInto(columnFamilyName)
						.value("volumeid", QueryBuilder.bindMarker())
						.value("volumeByteCount", QueryBuilder.bindMarker())
						.value("volumeCharacterCount", QueryBuilder.bindMarker())
						.value("structMetadata", QueryBuilder.bindMarker())
						.value("structMetadataType", QueryBuilder.literal("METS"))
						.value("lastModifiedTime", QueryBuilder.bindMarker())
						.value("cksumValidationTime", QueryBuilder.bindMarker());
 
				SimpleStatement insertStaticColsStmt = insertStaticCols.builder()
						.addNamedValue("volumeid", volumeId)
						.addNamedValue("volumeByteCount", volumeByteCount)
						.addNamedValue("volumeCharacterCount", volumeCharacterCount)
						.addNamedValue("structMetadata", volumeRecord.getMETSContents())
						.addNamedValue("lastModifiedTime", new Date())
						.addNamedValue("cksumValidationTime", new Date())
						.build();*/

				// the static columns are added using a separate statement, instead of combining them with the first page, 
				// as was done earlier
				SimpleStatement insertStaticColsStmt = SimpleStatement.newInstance(
				        "INSERT INTO " + columnFamilyName + " (volumeid, volumeByteCount, volumeCharacterCount, structMetadata, structMetadataType, lastModifiedTime, cksumValidationTime) VALUES (?, ?, ?, ?, ?, ?, ?)",
				        volumeId, volumeByteCount, volumeCharacterCount, volumeRecord.getMETSContents(), "METS", Instant.now(), Instant.now());
				
				batchStmtBuilder.addStatement(insertStaticColsStmt);
				
			} else {
				log.error("Cannot get entry from ZIP (zip file is probably empty) " + volumeZipFile.getAbsolutePath());
				if(!volumeRecord.getPageFilenameSet().isEmpty()) {
					pwEmptyZip.println(volumeId + " METS mismatch"); pwEmptyZip.flush();
				} else {
					pwEmptyZip.println(volumeId + " consistent with METS"); pwEmptyZip.flush();
				}
				return UpdatePagesResult.EMPTY_ZIP_ERROR;
			}
			
			// check if the pages listed in the METS file match the pages found in the ZIP file
			if (!pagesInZipFile.equals(volumeRecord.getPageFilenameSet())) {
				log.error("Pages listed in METS file do not match pages in ZIP file: volumeId = {}", volumeId);
				return UpdatePagesResult.METS_ZIP_MISMATCHED_PAGES_ERROR;
			}
			
			//8. then push the volume into cassandra
			batchStmtBuilder.setConsistencyLevel(DefaultConsistencyLevel.ONE);
			cassandraManager.executeWithoutRetry(batchStmtBuilder.build());
		} catch (IOException e) {
			log.error("IOException getting entry from ZIP " + volumeZipFile.getAbsolutePath(), e);
			return UpdatePagesResult.VOLUME_ZIP_ERROR;
		} catch (WriteFailureException e) {
			// a non-timeout error during a write query; happens when some of the replicas that are contacted by the coordinator 
			// reply with an error
			log.error("write failure for " + volumeId + ": " + e.getMessage());
			log.error("write failure for " + volumeId + ": " + e.getNumFailures() + " failures");
			log.error("write failure for " + volumeId + ": " + e.getCoordinator() + " coordinator");
			log.error("write failure for " + volumeId + ": " + e.getLocalizedMessage() + " local message");
			log.error("write failure for " + volumeId + ": " + e.getReceived() + " acks received");
			log.error("write failure for " + volumeId + ": " + e.getBlockFor() + " acks required");
			// log.error("write failure for " + volumeId + ": " + e.getHost() + " host"); // getHost() returns the coordinator of the request, not the failed host
			log.error("write failure for " + volumeId + ": " + e.getConsistencyLevel() + " consistency");
			log.error("write failure for " + volumeId + ": " + e.getWriteType() + " write type");
			return UpdatePagesResult.CASSANSDRA_WRITE_ERROR;
		} catch (WriteTimeoutException e) {
			// a server-side timeout during a write query
			log.error("write failure for " + volumeId + ": " + e.getMessage());
			return UpdatePagesResult.CASSANSDRA_WRITE_ERROR;
		} catch (DriverTimeoutException e) {
			log.error("driver timeout for " + volumeId + ": " + e.getMessage());
			return UpdatePagesResult.CASSANSDRA_WRITE_ERROR;
		} 
		
/*		catch (Exception e) {
			log.error("Exception in updatePages", e);
			return UpdatePagesResult.CASSANSDRA_WRITE_ERROR;
		}*/
		
		// volumeAdded = true;
		return UpdatePagesResult.SUCCESS;
	}

	// the following is used to create the volumezip value stored in Cassandra, from the contents of the zip file in the pairtree
/*	private ByteBuffer getByteBuffer(File file) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] buffer = new byte[32767];
		
		try {
			InputStream is = new FileInputStream(file);
			int read = -1;
			while((read = is.read(buffer)) > 0) {
				bos.write(buffer, 0, read);
			}
			is.close();
			
			byte[] bytes = bos.toByteArray();
			ByteBuffer bf = ByteBuffer.wrap(bytes);
			return bf;
		} catch (FileNotFoundException e) {
			log.error(file.getAbsolutePath() + " is not found");
		} catch (IOException e) {
			log.error("IOException while attempting to read " + file.getName());
		}
		return null;
	}*/

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
    
    public void close() {
    	pwEmptyZip.flush();
    	pwEmptyZip.close();
    	pwChecksumInfo.flush();
    	pwChecksumInfo.close();
    }
}
