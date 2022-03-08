package edu.indiana.d2i.ingest.cassandra;

import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

//import com.datastax.driver.core.PreparedStatement;
//import com.datastax.driver.core.ResultSet;
//import com.datastax.driver.core.querybuilder.QueryBuilder;
//import com.datastax.driver.core.querybuilder.Update;
//import com.datastax.oss.driver.api.querybuilder.BuildableQuery;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.util.Configuration;

/*
 * Class to set the value of the column containing semantic metadata (MARC) in the volume text column family in Cassandra.
 */
public class MarcIngester extends ColumnIngester<String> {
	
	private static Logger logger = LogManager.getLogger(MarcIngester.class);
	
	private String marcColumn;
	private String lastModTimeColName;
	private PreparedStatement marcUpdatePrepStmt = null;
	
	public MarcIngester() {
		super();
		this.marcColumn =  Configuration.getProperty(Constants.PK_MARC_COLUMN, Constants.DEFAULT_MARC_COLUMN);
		this.lastModTimeColName = Configuration.getProperty(Constants.PK_LAST_MOD_TIME_COLUMN, Constants.DEFAULT_LAST_MOD_TIME_COLUMN);
	}
	
	public boolean marcColFamilyExists() {
		return this.csdConnector.checkTableExist(this.volTextColFamily);
	}

	public String getMarcColFamily() {
		return this.volTextColFamily;
	}
	
	// update the marc column, and the lastModifiedTime column corresponding to the given volume id with the given marc record; the update takes
	// place only if the column family contains the volume id
	@Override
	public boolean ingest(String volumeId, String marc) {
		try {
			if (this.marcUpdatePrepStmt == null) {
				/*Update.IfExists updVolMarc = QueryBuilder.update(this.volTextColFamily)
						.with(QueryBuilder.set(this.marcColumn, QueryBuilder.bindMarker()))
						.and(QueryBuilder.set("semanticMetadataType", "MARC")) // hard code it there for marc ingester for now
						.and(QueryBuilder.set(this.lastModTimeColName, QueryBuilder.bindMarker()))
						.where(QueryBuilder.eq(this.volTextColFamilyKey, QueryBuilder.bindMarker()))
						.ifExists();*/			
				BuildableQuery updVolMarc = QueryBuilder.update(this.volTextColFamily)
						.setColumn(this.marcColumn, QueryBuilder.bindMarker())
						.setColumn("semanticMetadataType", QueryBuilder.literal("MARC"))
						.setColumn(this.lastModTimeColName, QueryBuilder.bindMarker())
						.whereColumn(this.volTextColFamilyKey)
						.isEqualTo(QueryBuilder.bindMarker())
						.ifExists();
				this.marcUpdatePrepStmt = csdConnector.prepare(updVolMarc.toString());
			}
			ResultSet rs = csdConnector.execute(this.marcUpdatePrepStmt.bind(marc, new Date(), volumeId));
			if (rs == null) {
				logger.error("MARC_INGESTER: error while trying to update {} for {} in table {}", this.marcColumn, volumeId, this.volTextColFamily);
				return false;
			} else {
				boolean res = this.getResultOfUpdateIfExists(rs);
				if (!res) {
					logger.error("MARC_INGESTER: {} not found in table {}; marc update failed", volumeId, this.volTextColFamily);
				}
				return res;
			}
		} catch (Exception e) {
			logger.error("MARC_INGESTER: Exception while trying to update {} for {} in table {}", this.marcColumn, volumeId, this.volTextColFamily, e);
			return false;
		}
	}
}
