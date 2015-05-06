package libisinworker;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.logging.Logger;
import junit.framework.TestCase;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author NaeemM
 */
public class DmtServiceTest extends TestCase {
    private Libisinworker worker;
    private LibisinUtil libisinUtils;
    private DmtService dmtRecords;
    private Properties dmtServiceConfig;
    private Logger serverLog;
    private String requestDir;     
    
    public DmtServiceTest(String testName) {
        super(testName);
        this.libisinDMTInit();
    }

    /* Initialize libisinworker */
    private void libisinDMTInit(){
        this.worker = new Libisinworker();        
        this.libisinUtils = new LibisinUtil();        
        this.serverLog = libisinUtils.createLogger(null, Libisinworker.class.getName());
        this.dmtRecords = new DmtService();  
        this.dmtServiceConfig = this.worker.getConfigurations("dmtservice", serverLog);
        this.requestDir = libisinUtils.createRequestDirectory();
        this.dmtRecords.requestLog = libisinUtils.createLogger(this.requestDir+"\\dmtunittest.txt", this.getClass().getName()); 
    }     
    
    /**
     * Test of mapData method, of class DmtService.
     */
    public void testMapData() throws Exception {
        System.out.println("mapData");
        String tempDir = System.getProperty("user.dir")+"\\temp";
        String records = tempDir+"\\00000012.json";
        String mappings = tempDir+"\\mappingrules.csv";
       
        boolean expResult = true;
        String result = this.dmtRecords.mapData(records, mappings, this.dmtServiceConfig);
        
        assertEquals(expResult, libisinUtils.isValidJSON(result) && result.contains("request_id"));

    }

    /**
     * Test of fetchOmekaDmt method, of class DmtService.
     */    
    public void testFetchOmekaDmt() {
        System.out.println("fetchOmekaDmt");
        String tempDir = System.getProperty("user.dir")+"\\temp";
        String records = tempDir+"\\00000012.json";
        String mappings = tempDir+"\\mappingrules.csv";        
        JSONParser parser = new JSONParser();
        int expResult = 0;
        int numberofRecords = 0;
                  
        try {
            String dmtMapResponse = this.dmtRecords.mapData(records, mappings, this.dmtServiceConfig);
            if(libisinUtils.isValidJSON(dmtMapResponse) && dmtMapResponse.contains("request_id")){
                JSONObject requestIdobj = (JSONObject) parser.parse(dmtMapResponse);
                String dmtRequestId = requestIdobj.get("request_id").toString();
                String dmtRecords = this.dmtRecords.fetchOmekaDmt(dmtRequestId, this.dmtServiceConfig);
                JSONArray recordArray = (JSONArray) parser.parse(dmtRecords);
                numberofRecords = recordArray.size();                
            }            
        } catch (IOException | ParseException | URISyntaxException ex) {
            System.out.println(ex.getMessage());
        } 

        assertTrue("No record retrieved from DMT service.", numberofRecords > expResult);

    }
    
}
