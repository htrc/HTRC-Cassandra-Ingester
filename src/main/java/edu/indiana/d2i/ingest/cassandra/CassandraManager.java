package edu.indiana.d2i.ingest.cassandra;

import java.net.InetSocketAddress;
import java.time.Duration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//import com.datastax.driver.core.BoundStatement;
//import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.Cluster.Builder;
//import com.datastax.driver.core.KeyspaceMetadata;
//import com.datastax.driver.core.PreparedStatement;
//import com.datastax.driver.core.ResultSet;
//import com.datastax.driver.core.Session;
//import com.datastax.driver.core.Statement;
//import com.datastax.driver.core.TableMetadata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;

import edu.indiana.d2i.ingest.util.Configuration;

public class CassandraManager {
	private static Logger log = LogManager.getLogger(CassandraManager.class);
	private static final CassandraManager manager = new CassandraManager();
	// private Cluster cluster ;
	private CqlSession session;
	private String[] contactPoints;
	private String volumeKeySpace;

/*	private CassandraManager() {	
		this.contactPoints = Configuration.getProperty("CONTACT_POINTS").split(",");
		Builder clusterBuilder = Cluster.builder(); //.addContactPoint("crow.soic.indiana.edu").build();
		for(String contactPoint : contactPoints) {
			System.out.println("contact point: " + contactPoint);
			clusterBuilder.addContactPoint(contactPoint);
		}
		this.cluster = clusterBuilder.build();
		this.volumeKeySpace = Configuration.getProperty("KEY_SPACE");
		System.out.println("volumeKeySpace = " + volumeKeySpace);
		this.session = this.cluster.connect(this.volumeKeySpace);
	}*/
	
	private CassandraManager() {	
		this.contactPoints = Configuration.getProperty("CONTACT_POINTS").split(",");
		CqlSessionBuilder builder = CqlSession.builder();
		for(String contactPoint : contactPoints) {
		  System.out.println("contact point: " + contactPoint);
		  builder.addContactPoint(new InetSocketAddress(contactPoint, 9042));
		}
		this.volumeKeySpace = Configuration.getProperty("KEY_SPACE");
		System.out.println("volumeKeySpace = " + volumeKeySpace);
		this.session = builder.withLocalDatacenter("Cassandra") // originally "datacenter1"; might need to change this to "DC1"
				.withKeyspace(CqlIdentifier.fromCql(this.volumeKeySpace))
				.build();
		System.out.println("CassandraManager instantiated successfully");
	}
	
/*	public boolean checkTableExist(String tableName) {
		KeyspaceMetadata keyspace = this.session.getMetadata().getKeyspace(volumeKeySpace);
		TableMetadata table = keyspace.getTable(tableName);
		if(table == null) {
			return false;
		} else {
			return true;
		}
	}*/
	
	public boolean checkTableExist(String tableName) {
		Metadata metadata = this.session.getMetadata();
		return metadata.getKeyspace(this.volumeKeySpace).flatMap(ks -> ks.getTable(tableName)).isPresent();
	}
	
	public ResultSet execute(String statementStr) {
		return session.execute(statementStr);
	}
	
	public static CassandraManager getInstance() {
			return manager;
	}
	
	public static void shutdown() {
		manager.session.close();
	}

	public PreparedStatement prepare(String statementTemplate) {
		return session.prepare(statementTemplate);
	}

	public PreparedStatement prepare(SimpleStatement stmt) {
		return session.prepare(stmt);
	}

	public ResultSet executeWithoutRetry(Statement stmt) {
		return session.execute(stmt);
/*		try {
			ResultSet results = session.execute(stmt);
			return results;
		} catch (Exception e) {
			log.error("Execution failed: " + stmt.toString());
			return null;
		}*/
	}
	
	public ResultSet execute(BoundStatement boundStatement) {
		//boolean executed = false;
		int maxAttempts = 3;
		while(/*!executed &&*/ maxAttempts > 0) {
			try{
				boundStatement.setTimeout(Duration.ofMillis(20000));
				ResultSet results = session.execute(boundStatement);
				//executed = true;
				return results;
			} catch (Exception e) {
				
				if (maxAttempts > 0) {
					maxAttempts--;
					log.warn("write time out error: " + e.getMessage());
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				} else {
					log.error("execution failed: " + boundStatement.toString());
				}
			}
		}
		return null;
	}
	
}
