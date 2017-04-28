package edu.indiana.d2i.ingest.cassandra;

import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.util.Configuration;

/*
 * Class to ingest to a column in the volume text column family using a simple CQL "UPDATE ... IF EXISTS" statement. This class is parameterized by 
 * the column data type. The "last modified time" column is also updated during the ingest. 
 */
public class SimpleColumnIngester<T> extends ColumnIngester<T> {	
	private static Logger logger = LogManager.getLogger(SimpleColumnIngester.class);
	
	private String colName;
	private String lastModTimeColName;
	private PreparedStatement colUpdatePrepStmt = null;
	
	public SimpleColumnIngester(String colName) {
		super();
		this.colName = colName;
		this.lastModTimeColName = Configuration.getProperty(Constants.PK_LAST_MOD_TIME_COLUMN, Constants.DEFAULT_LAST_MOD_TIME_COLUMN);
	}
	
	// update the column identified by "colName", and the lastModifiedTime column of the given volume id with the given value; the update 
	// takes place only if the column family contains the volume id; the CQL statement used for the update is
	//   UPDATE <table> SET <colName> = <colValue>, <lastModifiedTimeCol> = <currTime> WHERE <key> = <volumeId> IF EXISTS
	public boolean ingest(String volumeId, T colValue) {
		try {
			if (this.colUpdatePrepStmt == null) {
				Update.IfExists updColumn = QueryBuilder.update(this.volTextColFamily)
						.with(QueryBuilder.set(this.colName, QueryBuilder.bindMarker()))
						.and(QueryBuilder.set(this.lastModTimeColName, QueryBuilder.bindMarker()))
						.where(QueryBuilder.eq(this.volTextColFamilyKey, QueryBuilder.bindMarker()))
						.ifExists();
				this.colUpdatePrepStmt = csdConnector.prepare(updColumn.toString());
			}
			ResultSet rs = csdConnector.execute(this.colUpdatePrepStmt.bind(colValue, new Date(), volumeId));
			if (rs == null) {
				logger.error("SIMPLE_COLUMN_INGESTER: error while trying to update {} for {} in table {}", this.colName, volumeId, this.volTextColFamily);
				return false;
			} else {
				boolean res = this.getResultOfUpdateIfExists(rs);
				if (!res) {
					logger.error("SIMPLE_COLUMN_INGESTER: {} not found in table {}; update of column {} failed", volumeId, this.volTextColFamily, this.colName);
				}
				return res;
			}
		} catch (Exception e) {
			logger.error("SIMPLE_COLUMN_INGESTER: Exception while trying to update {} for {} in table {}", this.colName, volumeId, this.volTextColFamily, e);
			return false;
		}
	}
}
