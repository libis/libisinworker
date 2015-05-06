package libisinworker;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import junit.framework.TestCase;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author NaeemM
 */
public class CadataTest extends TestCase {
    
    private Libisinworker worker;
    private Cadata caRecords;
    private Properties caServerConfig;
    private Logger serverLog;
    private String requestDir;
    
    public CadataTest(String testName) {
        super(testName);
        this.libisinCAInit();
    }

    /* Initialize libisinworker */
    private void libisinCAInit(){
        LibisinUtil libisinUtils = new LibisinUtil();
        this.worker = new Libisinworker();        
        this.serverLog = libisinUtils.createLogger(null, Libisinworker.class.getName());
        this.caRecords = new Cadata();  
        this.caServerConfig = this.worker.getConfigurations("caserver", serverLog);
        this.requestDir = libisinUtils.createRequestDirectory();
        this.caRecords.requestLog = libisinUtils.createLogger(this.requestDir+"\\unittest.txt", this.getClass().getName()); 
    }   
    
    /**
     * Test of getSetData method, of class Cadata.
     */
    public void testGetSetData() {
        System.out.println("getSetData");
        String setName = "00000012";
        String bundle = "{\"bundles\":{\"ca_objects.idno\":{\"convertCodesToDisplayText\":true,\"maximum_length\":100},\"ca_objects.object_id\":{\"convertCodesToDisplayText\":true,\"maximum_length\":100},\"ca_objects.cagObjectnaamInfo\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_objects.cagObjectnaamInfo.objectNaam\"},\"ca_objects.moveObjectnaam\":{\"convertCodesToDisplayText\":true,\"maximum_length\":2048},\"ca_places.georeference\":{\"convertCodesToDisplayText\":true,\"coordinates\":true,\"returnAsArray\":true,\"template\":\"^ca_places.georeference\"},\"ca_objects.preferred_labels\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_objects.preferred_labels.name\"},\"ca_objects.inhoudBeschrijving\":{\"convertCodesToDisplayText\":true},\"ca_entities\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_entities.preferred_labels\"},\"dc:provenance\":{\"template\":\"^ca_entities.preferred_labels%delimiter=;_%restrictToRelationshipTypes=295|304\"},\"ca_collections\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_collections.preferred_labels\"},\"ca_entities.preferred_labels.displayname\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_entities.preferred_labels.displayname\"},\"dc:maker\":{\"template\":\"^ca_entities.preferred_labels.displayname%delimiter=;_%restrictToRelationshipTypes=292|649|652|655|661|664|667|766|673|676|679|685|691|694|697|784|703|706|712|715|718|721|724|733|736|739|742|748|751|754|757|760|763|769|772|775|778|781|787|790|793|796|799|802|805|808|811|814|817|820|826|829|832|835|838|841|844\"},\"ca_objects.objectVervaardigingInfo\":{\"convertCodesToDisplayText\":true,\"maximum_length\":2048},\"ca_objects.objectVervaardigingInfo.objectVervaardigingPlace\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_objects.objectVervaardigingInfo.objectVervaardigingPlace\",\"maximum_length\":2048},\"dc:references\":{\"template\":\"^ca_occurrences.preferred_labels.name%delimiter=;_%restrictToTypes=271%restrictToRelationshipTypes=388\"},\"ca_objects.objectVervaardigingInfo.objectVervaardigingDate\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_objects.objectVervaardigingInfo.objectVervaardigingDate\",\"maximum_length\":2048},\"ca_objects.digitoolUrl\":{\"convertCodesToDisplayText\":true,\"returnAsArray\":true},\"ca_objects.creativecommons\":{\"convertCodesToDisplayText\":true,\"maximum_length\":2048},\"ca:tags\":{\"template\":\"^ca_list_items.preferred_labels.name%delimiter=;_%restrictToRelationshipTypes=457\"}}}";
        String setRecordsType = "objecten";    
        
        String expResult = setName+".json";
        
        String outputFile = this.caRecords.getSetData(setName, this.caServerConfig, bundle, setRecordsType, this.requestDir);
        File f = new File(outputFile);
        
        assertEquals(expResult, f.getName()); // Match file names
    }

    /**
     * Test number of records in result(getSetData method, of class Cadata).
     */
    public void testNumberofRecords() {
        System.out.println("getSetData - Nubmer of records");
        String setName = "0000001256";
        String bundle = "{\"bundles\":{\"ca_objects.idno\":{\"convertCodesToDisplayText\":true,\"maximum_length\":100},\"ca_objects.object_id\":{\"convertCodesToDisplayText\":true,\"maximum_length\":100},\"ca_objects.cagObjectnaamInfo\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_objects.cagObjectnaamInfo.objectNaam\"},\"ca_objects.moveObjectnaam\":{\"convertCodesToDisplayText\":true,\"maximum_length\":2048},\"ca_places.georeference\":{\"convertCodesToDisplayText\":true,\"coordinates\":true,\"returnAsArray\":true,\"template\":\"^ca_places.georeference\"},\"ca_objects.preferred_labels\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_objects.preferred_labels.name\"},\"ca_objects.inhoudBeschrijving\":{\"convertCodesToDisplayText\":true},\"ca_entities\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_entities.preferred_labels\"},\"dc:provenance\":{\"template\":\"^ca_entities.preferred_labels%delimiter=;_%restrictToRelationshipTypes=295|304\"},\"ca_collections\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_collections.preferred_labels\"},\"ca_entities.preferred_labels.displayname\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_entities.preferred_labels.displayname\"},\"dc:maker\":{\"template\":\"^ca_entities.preferred_labels.displayname%delimiter=;_%restrictToRelationshipTypes=292|649|652|655|661|664|667|766|673|676|679|685|691|694|697|784|703|706|712|715|718|721|724|733|736|739|742|748|751|754|757|760|763|769|772|775|778|781|787|790|793|796|799|802|805|808|811|814|817|820|826|829|832|835|838|841|844\"},\"ca_objects.objectVervaardigingInfo\":{\"convertCodesToDisplayText\":true,\"maximum_length\":2048},\"ca_objects.objectVervaardigingInfo.objectVervaardigingPlace\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_objects.objectVervaardigingInfo.objectVervaardigingPlace\",\"maximum_length\":2048},\"dc:references\":{\"template\":\"^ca_occurrences.preferred_labels.name%delimiter=;_%restrictToTypes=271%restrictToRelationshipTypes=388\"},\"ca_objects.objectVervaardigingInfo.objectVervaardigingDate\":{\"convertCodesToDisplayText\":true,\"template\":\"^ca_objects.objectVervaardigingInfo.objectVervaardigingDate\",\"maximum_length\":2048},\"ca_objects.digitoolUrl\":{\"convertCodesToDisplayText\":true,\"returnAsArray\":true},\"ca_objects.creativecommons\":{\"convertCodesToDisplayText\":true,\"maximum_length\":2048},\"ca:tags\":{\"template\":\"^ca_list_items.preferred_labels.name%delimiter=;_%restrictToRelationshipTypes=457\"}}}";
        String setRecordsType = "objecten";    
        
        int expResult = 0;
        
        String outputFile = this.caRecords.getSetData(setName, this.caServerConfig, bundle, setRecordsType, this.requestDir);
        
        JSONParser parser = new JSONParser();
        int numberofRecords = 0;
        try {
            Object obj = parser.parse(new FileReader(outputFile));
            JSONObject jsonObject = (JSONObject) obj;          
            JSONArray records = (JSONArray) jsonObject.get("results");
            if(records != null)
                numberofRecords = records.size();
        } catch (ParseException | IOException ex) {
             System.out.println(ex.getMessage());
        }
       
        assertTrue("No record retrieved from Collective Access.", numberofRecords > expResult);
    }    
    
}
