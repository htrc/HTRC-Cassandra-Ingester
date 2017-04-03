package edu.indiana.d2i.ingest.cassandra;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.util.Configuration;

public class MarcIngester {
	private static Logger logger = LogManager.getLogger(MarcProcessor.class);
	
	private CassandraManager csdConnector;
	private String marcColFamily;
	private String marcColFamilyKey;
	private String marcColumn;
	private PreparedStatement marcUpdatePrepStmt = null;
	
	public MarcIngester() {
		this.csdConnector = CassandraManager.getInstance();
		this.marcColFamily = Configuration.getProperty(Constants.PK_MARC_COLFAMILY, Constants.DEFAULT_MARC_COLFAMILY);
		this.marcColFamilyKey =  Configuration.getProperty(Constants.PK_MARC_COLFAMILY_KEY, Constants.DEFAULT_MARC_COLFAMILY_KEY);
		this.marcColumn =  Configuration.getProperty(Constants.PK_MARC_COLUMN, Constants.DEFAULT_MARC_COLUMN);
	}
	
	public boolean marcColFamilyExists() {
		return this.csdConnector.checkTableExist(this.marcColFamily);
	}

	public String getMarcColFamily() {
		return this.marcColFamily;
	}
	
	// updates the marc column corresponding to the given volume id with the given marc record; the update takes place only if the column family
	// contains the volumeid
	public boolean ingestVolumeMarc(String volumeid, String marc) {
		try {
			if (this.marcUpdatePrepStmt == null) {
				Update.IfExists updVolMarc = QueryBuilder.update(this.marcColFamily)
						.with(QueryBuilder.set(this.marcColumn, QueryBuilder.bindMarker()))
						.and(QueryBuilder.set("semanticMetadataType", "MARC")) // hard code it there for marc ingester for now
						.where(QueryBuilder.eq(this.marcColFamilyKey, QueryBuilder.bindMarker()))
						.ifExists();
				this.marcUpdatePrepStmt = csdConnector.prepare(updVolMarc.toString());
			}
			ResultSet rs = csdConnector.execute(this.marcUpdatePrepStmt.bind(marc, volumeid));
			if (rs == null) {
				logger.error("MARC_INGESTER: error while trying to update {} for {} in table {}", this.marcColFamily, volumeid, this.marcColFamily);
				return false;
			} else {
				boolean res = this.getResultOfUpdateIfExists(rs);
				if (!res) {
					logger.error("MARC_INGESTER: {} not found in table {}; marc update failed", volumeid, this.marcColFamily);
				}
				return res;
			}
		} catch (Exception e) {
			logger.error("MARC_INGESTER: Exception while trying to update {} for {} in table {}", this.marcColFamily, volumeid, this.marcColFamily, e);
			return false;
		}
	}
	
	private boolean getResultOfUpdateIfExists(ResultSet rs) {
		// the ResultSet of an "UPDATE ... IF EXISTS" statement contains one row, with one boolean column named "[applied]"
		Row row = rs.one(); 
		if (row != null) {
			try {
				return row.getBool(0);
			} catch (Exception e) {
				// IndexOutOfBoundsException, InvalidTypeException
				return false;
			}
		}
		return false;
	}
}
