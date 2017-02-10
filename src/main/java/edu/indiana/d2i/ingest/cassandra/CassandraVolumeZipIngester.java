package edu.indiana.d2i.ingest.cassandra;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.Ingester;
import edu.indiana.d2i.ingest.util.Configuration;
import edu.indiana.d2i.ingest.util.Tools;

public class CassandraVolumeZipIngester extends Ingester {
	private static Logger log = LogManager.getLogger(CassandraVolumeZipIngester.class);
	private CassandraManager cassandraManager;
	private PreparedStatement insertStatement;
	private String columnFamilyName;
	
	public CassandraVolumeZipIngester() {
		cassandraManager = CassandraManager.getInstance();
		columnFamilyName = Configuration.getProperty(Constants.PK_VOLUME_ZIP_COLUMN_FAMILY);
		if(! cassandraManager.checkTableExist(columnFamilyName)) {
			String createTableStr = "CREATE TABLE " + columnFamilyName + " ("
		    		+ "volumeID text PRIMARY KEY, "
		    		+ "zippedContent blob, "
		    		+ "mets text, "
		    		+ "marc text, "
		    		+ "byteCount bigint) ";
			cassandraManager.execute(createTableStr);
		}
		insertStatement = cassandraManager.prepare("INSERT INTO " + columnFamilyName + " (volumeID, zippedContent, mets, marc, byteCount)" + "VALUES(?,?,?,?,?);");
	}
	

	public boolean ingestOne(String volumeId) {
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
		// this is to be implemented
		String volumeMarc = Tools.getMarcString(volumeId);
		if(volumeZipFile == null || volumeMetsFile == null || !volumeZipFile.exists() || !volumeMetsFile.exists()) {
			log.error("zip file or mets file does not exist for " + volumeId);
			return false;
		}
		
		boolean volumeAdded = updateVolumeZip(volumeId, volumeZipFile, volumeMetsFile, volumeMarc);
		if(volumeAdded) {
			log.info("zip ingested successfully " + volumeId);
		} else {
			log.info("zip ingest failed " + volumeId);
		}
		return volumeAdded;
	}


	private boolean updateVolumeZip(String volumeId, File volumeZipFile, File volumeMetsFile, String volumeMarc) {
		BoundStatement boundStatement = new BoundStatement(insertStatement);
		ByteBuffer zipBinaryContent = getByteBuffer(volumeZipFile);
		ByteBuffer metsBinaryContent = getByteBuffer(volumeMetsFile);
		
		if(zipBinaryContent != null && metsBinaryContent != null && volumeMarc != null) {
			String volumeMets = null;
			try {
				volumeMets = new String(metsBinaryContent.array(), "utf-8");
			} catch (UnsupportedEncodingException e) {
				log.error("cannot read mets content for " + volumeId);
				log.error("unsupported encoding", e);
			}
			boundStatement.bind(volumeId, zipBinaryContent, volumeMets, volumeMarc, volumeZipFile.length());
		} else {
			return false;
		}
		
		ResultSet result = cassandraManager.execute(boundStatement);
		return result != null;
	}
	
	private ByteBuffer getByteBuffer(File file) {
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
	}

}
