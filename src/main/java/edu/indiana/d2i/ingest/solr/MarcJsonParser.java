package edu.indiana.d2i.ingest.solr;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class MarcJsonParser {

	static class VolumeInfo {
		private String volumeId;
		private String persistentId;
		private String htId; 
		private Set<String> countryOfPub = new HashSet<String>();
		private Set<String> isbn = new HashSet<String>() ;
		private Set<String> oclc = new HashSet<String>();
		private Set<String> language = new HashSet<String>();
		private Set<String> publishDate = new HashSet<String>();
		private Set<String> title = new HashSet<String>();
		private Set<String> author = new HashSet<String>();
		
		public Set<String> getCountryOfPub() {
			return countryOfPub;
		}

		public Set<String> getIsbn() {
			return isbn;
		}

		public Set<String> getOclc() {
			return oclc;
		}

		public Set<String> getLanguage() {
			return language;
		}

		public Set<String> getPublishDate() {
			return publishDate;
		}

		public Set<String> getTitle() {
			return title;
		}

		public Set<String> getAuthor() {
			return author;
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("volumeId: ").append(volumeId).append('\n')
			.append("htId: ").append(htId).append('\n')
			.append("isbn: ").append(isbn).append('\n')
			.append("oclc: ").append(oclc).append('\n')
			.append("language: ").append(language).append('\n')
			.append("persistentId:").append(persistentId).append('\n')
			.append("publishDate:").append(publishDate).append('\n')
			.append("author").append(author).append('\n')
			.append("title: ").append(title);
			return sb.toString();
		}
		
		public String getHtId() {
			return htId;
		}
		public void setHtId(String htId) {
			this.htId = htId;
		}
		public void addCountryOfPub(String _country) {
			countryOfPub.add(_country);
		}
		public void addIsbn(String _isbn) {
			isbn.add(_isbn);
		}
		public void addOclc(String _oclc) {
			oclc.add(_oclc);
		}
		public void addLanguage(String _language) {
			language.add(_language);
		}
		public void addPublishDate(String _publishDate) {
			publishDate.add(_publishDate);
		}
		
		public void addTitle(String _title) {
			title.add(_title);
		}
		public void addAuthor(String _author) {
			author.add(_author);
		}
				
		public String getVolumeId() {
			return volumeId;
		}
		public void setVolumeId(String volumeId) {
			this.volumeId = volumeId;
		}
		public String getPersistentId() {
			return persistentId;
		}
		public void setPersistentId(String persistentId) {
			this.persistentId = persistentId;
		}
	}
	
	public static Set<String> getStringValue(String fieldCode, List<String> subfieldCodes, JSONObject fieldJson) {
		JSONArray subfields = (JSONArray)((JSONObject)fieldJson.get(fieldCode)).get("subfields");
		Set<String> results = new HashSet<String>();
		for(String subfieldCode : subfieldCodes) {
			String value = (String)Stream.of(subfields.toArray()).map(subfield -> (JSONObject)subfield)
					.filter(subfieldJson -> subfieldJson.containsKey(subfieldCode)).map(subfieldJson -> subfieldJson.get(subfieldCode))
					.findFirst().orElse(null);
			if(value != null) {
				results.add(value);
			}
		}
		return results;
	}
	
	public static String getConcatStringValue(String fieldCode, List<String> subfieldCodes, JSONObject fieldJson) {
		JSONArray subfields = (JSONArray)((JSONObject)fieldJson.get(fieldCode)).get("subfields");
		StringBuilder sb = new StringBuilder();
		for(String subfieldCode : subfieldCodes) {
			String value = (String)Stream.of(subfields.toArray()).map(subfield -> (JSONObject)subfield)
					.filter(subfieldJson -> subfieldJson.containsKey(subfieldCode)).map(subfieldJson -> subfieldJson.get(subfieldCode))
					.findFirst().orElse(null);
			if(value != null) {
				sb.append(value);
			}
		}
		return sb.toString();
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException, FileNotFoundException, IOException, ParseException {
		JSONParser jsonParser = new JSONParser();
		JSONObject json = (JSONObject)jsonParser.parse(new InputStreamReader(new FileInputStream("x.json"), "UTF-8"));
		VolumeInfo volume = parse(json);
		System.out.println(volume);
	}
	public static VolumeInfo parse(JSONObject json) {
		
		JSONArray fields = (JSONArray)json.get("fields");
		Object[] fieldObjects = fields.toArray();
		VolumeInfo volume = new VolumeInfo();
		Stream.of(fieldObjects).map(fieldObj -> (JSONObject)fieldObj)
		.forEach(fieldJson -> {
			if(fieldJson.containsKey("974")) {
				List<String> subfieldCodes = new LinkedList<String>();
				subfieldCodes.add("u");
				Set<String> results = getStringValue("974", subfieldCodes, fieldJson);
		//		System.out.println(results);
				if(results != null && results.size()== 1) {
					volume.setVolumeId(results.iterator().next());
				}
			} else if(fieldJson.containsKey("035")) {
				List<String> subfieldCodes = new LinkedList<String>();
				subfieldCodes.add("a");
				subfieldCodes.add("z");
				Set<String> results = getStringValue("035", subfieldCodes, fieldJson);
				results.stream().filter(result -> result.matches("^\\(OCoLC\\)(?:oclc|ocolc|ocm|ocn)*?(\\d+)"))
				.forEach(result -> volume.addOclc(result));
			} else if(fieldJson.containsKey("020")) {
				List<String> subfieldCodes = new LinkedList<String>();
				subfieldCodes.add("a");
				subfieldCodes.add("z");
				Set<String> results = getStringValue("020", subfieldCodes, fieldJson);
				returnValidISBNs(results).stream().forEach(result -> volume.addIsbn(result));
			} else if(fieldJson.containsKey("008")) {
			//	System.out.println(fieldJson.get("008").toString().substring(7, 11).trim());
				String field008 = fieldJson.get("008").toString();
				if(field008.length() >= 11) {
					volume.addPublishDate(fieldJson.get("008").toString().substring(7, 11).trim());
				}
				if(field008.length() >= 18) {
					volume.addCountryOfPub(fieldJson.get("008").toString().substring(15, 18).trim());
				}
				if(field008.length() >= 38) {
					volume.addLanguage(fieldJson.get("008").toString().substring(35, 38).trim());
				}
			} else if(fieldJson.containsKey("001")) {
			//	System.out.println(fieldJson.get("001").toString().trim());
				volume.setHtId(fieldJson.get("001").toString().trim());
			} else if(fieldJson.containsKey("100")) {
				List<String> subfieldCodes = new LinkedList<String>();
				subfieldCodes.add("a");
				subfieldCodes.add("b");
				subfieldCodes.add("c");
				subfieldCodes.add("d");
				String author = getConcatStringValue("100", subfieldCodes, fieldJson);
				if(author!=null && !author.equals("")) {
					volume.addAuthor(author);
				}
			} else if(fieldJson.containsKey("110")) {
				List<String> subfieldCodes = new LinkedList<String>();
				subfieldCodes.add("a");
				subfieldCodes.add("b");
				subfieldCodes.add("c");
				subfieldCodes.add("d");
				String author = getConcatStringValue("110", subfieldCodes, fieldJson);
				if(author!=null && !author.equals("")) {
					volume.addAuthor(author);
				}
			} else if(fieldJson.containsKey("700")) {
				List<String> subfieldCodes = new LinkedList<String>();
				subfieldCodes.add("a");
				subfieldCodes.add("b");
				subfieldCodes.add("c");
				subfieldCodes.add("d");
				String author = getConcatStringValue("700", subfieldCodes, fieldJson);
				if(author!=null && !author.equals("")) {
					volume.addAuthor(author);
				}
			}/* else if(fieldJson.containsKey("710")) {
				List<String> subfieldCodes = new LinkedList<String>();
				subfieldCodes.add("a");
				subfieldCodes.add("b");
				subfieldCodes.add("c");
				subfieldCodes.add("d");
				String author = getConcatStringValue("710", subfieldCodes, fieldJson);
				if(author!=null && !author.equals("")) {
					volume.addAuthor(author);
				}
			}*/ else if(fieldJson.containsKey("111")) {
				List<String> subfieldCodes = new LinkedList<String>();
				subfieldCodes.add("a");
				subfieldCodes.add("b");
				subfieldCodes.add("c");
				String author = getConcatStringValue("111", subfieldCodes, fieldJson);
				if(author!=null && !author.equals("")) {
					volume.addAuthor(author);
				}
			} else if(fieldJson.containsKey("711")) {
				List<String> subfieldCodes = new LinkedList<String>();
				subfieldCodes.add("a");
				subfieldCodes.add("b");
				subfieldCodes.add("c");
				String author = getConcatStringValue("711", subfieldCodes, fieldJson);
				if(author!=null && !author.equals("")) {
					volume.addAuthor(author);
				}
			} else if(fieldJson.containsKey("245")) {
				List<String> subfieldCodes = new LinkedList<String>();
				subfieldCodes.add("a");
				subfieldCodes.add("b");
				subfieldCodes.add("c");
				subfieldCodes.add("d");
				subfieldCodes.add("e");
				subfieldCodes.add("f");
				subfieldCodes.add("g");
				subfieldCodes.add("h");
				subfieldCodes.add("k");
				subfieldCodes.add("n");
				subfieldCodes.add("p");
				String title = getConcatStringValue("245", subfieldCodes, fieldJson);
				if(title!= null && !title.equals("")) {
					volume.addTitle(title);
				}
			}
		});
		return volume;
	}

	public static Set<String> returnValidISBNs(Set<String> candidates)
    {
        // NOTE 1: last digit of ISBN is a check digit and may be "X" (0,1,2,3,4,5,6.7.8.9.X)
        // NOTE 2: ISBN can be 10 or 13 digits (and may end with X).
        // NOTE 3: 13 digit ISBN must start with 978 or 979.
        // NOTE 4: there may be text after the ISBN, which should be removed 
        Set<String> isbnSet = new LinkedHashSet<String>();
        Pattern p10 = Pattern.compile("^\\d{9}[\\dX].*");
        Pattern p13 = Pattern.compile("^(978|979)\\d{9}[X\\d].*");
        // p13any matches a 13 digit isbn pattern without the correct prefix
        Pattern p13any = Pattern.compile("^\\d{12}[X\\d].*");
        Iterator<String> iter = candidates.iterator();
        while (iter.hasNext()) {
            String value = (String)iter.next().trim();
            // check we have the right pattern, and remove trailing text
            if (p13.matcher(value).matches()) 
                isbnSet.add(value.substring(0, 13));
            else if (p10.matcher(value).matches() && !p13any.matcher(value).matches()) 
                isbnSet.add(value.substring(0, 10));
        }
        return isbnSet;            
    }
}
