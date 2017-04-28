package edu.indiana.d2i.ingest.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import edu.indiana.d2i.ingest.Constants;
import edu.indiana.d2i.ingest.util.Configuration;

/*
 * Class that ingests to a single column in the volume text column family. This is the base class for classes that set the values
 * of the marc column and the access level column, after the ingest of the volume text data into Cassandra. This class is parameterized by the
 * column data type.
 */
public abstract class ColumnIngester<T> {
	CassandraManager csdConnector;
	String volTextColFamily;
	String volTextColFamilyKey;

	ColumnIngester() {
		this.csdConnector = CassandraManager.getInstance();
		this.volTextColFamily = Configuration.getProperty(Constants.PK_VOLUME_TEXT_COLUMN_FAMILY, Constants.DEFAULT_VOLUME_TEXT_COLUMN_FAMILY);
		this.volTextColFamilyKey = Configuration.getProperty(Constants.PK_VOLUME_TEXT_COLUMN_FAMILY_KEY, Constants.DEFAULT_VOLUME_TEXT_COLUMN_FAMILY_KEY);
	}

	public abstract boolean ingest(String volumeId, T colValue);

	boolean getResultOfUpdateIfExists(ResultSet rs) {
		// the ResultSet of an "UPDATE ... IF EXISTS" statement contains one row,
		// with one boolean column named "[applied]"
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
