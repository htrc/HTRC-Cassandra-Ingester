package edu.indiana.d2i.ingest.solr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import edu.indiana.d2i.ingest.Ingester;
import edu.indiana.d2i.ingest.cassandra.MarcProcessor;
import edu.indiana.d2i.ingest.solr.MarcJsonParser.VolumeInfo;
import edu.indiana.d2i.ingest.util.Configuration;

public class SolrMetadtaIngester extends Ingester {
	private static Logger log = LogManager.getLogger(SolrMetadtaIngester.class);
	private static final String SOLR_EPR;
	private SolrClient solrCli;
	private PrintWriter pwSuccess;
	private PrintWriter pwFailure;
	static {
		SOLR_EPR = Configuration.getProperty("SOLR_URL");
	}
	
	public SolrMetadtaIngester() {
		solrCli = new HttpSolrClient.Builder(SOLR_EPR).build(); 
		try {
			pwSuccess = new PrintWriter(Configuration.getProperty("SOLR_INGESTER_SUCCESS"));
			pwFailure = new PrintWriter(Configuration.getProperty("SOLR_INGESTER_FAILURE"));
		} catch (FileNotFoundException e) {
			log.error("id logs for solr ingest not found", e);
		}
		
	}
	
	public void ingest(List<String> volumes) {
		List<File> marcFiles = new LinkedList<File>();
		Stream.of(Configuration.getProperty("MARC-JSON-FILES").split(",")).forEach(marcName -> {
			marcFiles.add(new File(Configuration.getProperty("MARC-JSON-FILES-FOLDER"), marcName));
		});
		Set<String> volumeIdSet = new HashSet<String>(volumes);
		for(File marcFile : marcFiles) {
			System.out.println("scanning " + marcFile.getAbsolutePath());
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(marcFile), "UTF-8"));
				String marcJsonStr = null;
				while((marcJsonStr = br.readLine()) != null) {
					JSONParser jsonParser = new JSONParser();
					JSONObject marcJson = (JSONObject)jsonParser.parse(marcJsonStr);
					String volumeId = new MarcProcessor().getVolumeIdFromMarc(marcJson);
					log.info("checking " + volumeId);
					//String volumeId = volume.getVolumeId();
					if(volumeId != null) {
						if(volumeIdSet.contains(volumeId)) {
							log.info("parsing " + volumeId);
							VolumeInfo volume = MarcJsonParser.parse(marcJson);
							boolean status = addToSolr(volume);
							if(status) {
								pwSuccess.println(volumeId); pwSuccess.flush();
							} else {
								pwFailure.println(volumeId); pwFailure.flush();
							}
							log.info("ingested " + volumeId + " to solr: " + status);
						}
					}
				}
				br.close();
			} catch (FileNotFoundException e) {
				log.error("MARC file not found for solr ingest", e);
			} catch (UnsupportedEncodingException e) {
				log.error("UTF-8 not supported", e);
			} catch (IOException e) {
				log.error("exception when reading marc files", e);
			} catch (ParseException e) {
				log.error("error parsing json line", e);
			}
		}
	}
	
	public void close() {
		pwSuccess.flush();pwSuccess.close();
		pwFailure.flush();pwFailure.close();
		try {
			solrCli.close();
		} catch (IOException e) {
			log.error("error when closing solr client", e);
		}
	}
	
	private boolean addToSolr(VolumeInfo volume) {
		SolrInputDocument document = new SolrInputDocument();
		document.addField("volumeId", volume.getVolumeId());
		document.addField("htId", volume.getHtId());
		document.addField("countryOfPub", AcronymToFullNameMapper.country.getFullNames(volume.getCountryOfPub()));
		document.addField("isbn", volume.getIsbn());
		document.addField("oclc", volume.getOclc());
		document.addField("persistentId", volume.getPersistentId());
		document.addField("language", AcronymToFullNameMapper.language.getFullNames(volume.getLanguage()));
		document.addField("publishDate", volume.getPublishDate());
		document.addField("title", volume.getTitle());
		document.addField("author", volume.getAuthor());
		
		try {
			solrCli.add(document);
			log.info("indexing " + volume.getVolumeId());
			// Remember to commit your changes!
			UpdateResponse result = solrCli.commit();
			if(result.getStatus() == 0) {
				return true;
			} else {
				return false;
			}
		} catch (SolrServerException | IOException e) {
			log.error("exception when adding metadta for " + volume.getVolumeId(), e);
			return false;
		} catch (Exception e) {
			log.error("other unexpected exceptions: ", e);
			return false;
		}
	}

	@Override
	public boolean ingestOne(String volumeId) {
		// ingest one is simply not worth it since it requires to linearly scan through 3 big .json file for just a single volume
		// leave it returning false always. 
		return false;
	}
}
