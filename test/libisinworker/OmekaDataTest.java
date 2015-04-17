package libisinworker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
public class OmekaDataTest extends TestCase {
    
    private Libisinworker worker;
    private OmekaData omekaRecords;
    private Properties libisinWorkerConfig;
    private Logger serverLog;
    
    public OmekaDataTest(String testName) {
        super(testName);
        this.libisinInit();
    }
    
    /* Initialize libisinwork */
    private void libisinInit(){
        LibisinUtil libisinUtils = new LibisinUtil();
        this.worker = new Libisinworker();
        this.serverLog = libisinUtils.createLogger(null, Libisinworker.class.getName());      
        this.omekaRecords = new OmekaData(this.worker.getConfigurations("omekaserver", serverLog));                     
        this.omekaRecords.requestLog = libisinUtils.createLogger(System.getProperty("user.dir")+"\\temp\\unittest.txt", this.getClass().getName());
    }

    /**
     * Test of removeDigiToolImage method, of class OmekaData.
     * Test if images of an item are removed correctly.
     */
    public void testRemoveDigiToolImage() {
        System.out.println("removeDigiToolImage");
        String itemId = "61355";       
        boolean expResult = true;
        boolean result = this.omekaRecords.removeDigiToolImage(itemId);
        assertEquals(expResult, result);
    }

    /**
     * Test of addDigiToolImage method, of class OmekaData.
     * Test if digitool images are added correctly to an omeka item.
     */
    public void testAddDigiToolImage() {
        System.out.println("addDigiToolImage");
        String item = "61355"; //63488
        JSONObject itemObject = new JSONObject();    
        itemObject.put("id", item);
        
        List<String> pids = new ArrayList<>(Arrays.asList("2263067","2263070", "2263073"));
        int expResult = pids.size();

        /* Set pids of images */
        for(String strPid: pids){
            System.out.println(strPid);
            this.omekaRecords.setDigiToolPid(strPid);
        }
        this.omekaRecords.addDigiToolImage(itemObject.toString());

        /* Retrieve digitool images for this item */
        JSONArray responseBody = (JSONArray)this.omekaRecords.getDigiToolPid(item);
        int result = responseBody.size();
        
        assertEquals(expResult, result);
    }
    
    /**
     * Test of updateItem method, of class OmekaData
     * Test if an omeka item is correctly updated.
     */
    public void testupdateItem(){
        System.out.println("updateItem");  
       
        String strItem = "{\"id\":\"61355\","
                + "\"tags\":[{\"name\":\"zuivel\"},{\"name\":\" marketing\"},{\"name\":\" tentoonstelling\"}],"
                + "\"added\":null,\"featured\":\"true\",\"collection\":null,\"public\":\"true\","
                + "\"element_texts\":["
                + "{\"element\":{\"id\":\"43\"},\"text\":\"00000012\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"252\"},\"text\":\"34\",\"element_set\":{\"id\":\"3\"},\"html\":true},"
                + "{\"element\":{\"id\":\"229\"},\"text\":\"foto\",\"element_set\":{\"id\":\"3\"},\"html\":true},"
                + "{\"element\":{\"id\":\"50\"},\"text\":\"Foto van de nationale propaganda voor zuivelproducten uit 1935\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"41\"},\"text\":\"Deze zwart\\/witfoto  1935.\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"124\"},\"text\":\"KADOC - KU Leuven\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"250\"},\"text\":\"Archief Boerenbond\",\"element_set\":{\"id\":\"3\"},\"html\":true},"
                + "{\"element\":{\"id\":\"39\"},\"text\":\"Boerenbond\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"115\"},\"text\":\"Leuven - 3000 \",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"40\"},\"text\":\"1930 - 1940\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"98\"},\"text\":\"Attribution No Derivatives (CC BY-ND)\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"109\"},\"text\":\"Van Leuven, Oude tuinbouwvoorwerpen, 1995.\",\"element_set\":{\"id\":\"1\"},\"html\":true}],\"item_type\":{\"id\":\"6\"},\"modified\":null}";
              
        boolean expResult = true;
        try {
            JSONObject omekaObject = (JSONObject)new JSONParser().parse(strItem);
            boolean result = this.omekaRecords.updateItem(omekaObject, "item");
            assertEquals(expResult, result);
            
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
     * Test of addItem method, of class OmekaData.
     * Test if an items is correctly added to omeka.
     */    
    public void testaddItem(){        
        System.out.println("addItem");
            
        String strItem = "{"
                + "\"tags\":[{\"name\":\"zuivel\"},{\"name\":\" marketing\"},{\"name\":\" tentoonstelling\"}],"
                + "\"added\":null,\"featured\":\"true\",\"collection\":null,\"public\":\"true\","
                + "\"element_texts\":["
                + "{\"element\":{\"id\":\"43\"},\"text\":\"00000012\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"252\"},\"text\":\"34555555\",\"element_set\":{\"id\":\"3\"},\"html\":true}," //Object_id
                + "{\"element\":{\"id\":\"229\"},\"text\":\"foto\",\"element_set\":{\"id\":\"3\"},\"html\":true},"
                + "{\"element\":{\"id\":\"50\"},\"text\":\"Foto van de nationale propaganda voor zuivelproducten uit 1935\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"41\"},\"text\":\"Deze zwart\\/witfoto  1935.\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"124\"},\"text\":\"KADOC - KU Leuven\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"250\"},\"text\":\"Archief Boerenbond\",\"element_set\":{\"id\":\"3\"},\"html\":true},"
                + "{\"element\":{\"id\":\"39\"},\"text\":\"Boerenbond\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"115\"},\"text\":\"Leuven - 3000 \",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"40\"},\"text\":\"1930 - 1940\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"98\"},\"text\":\"Attribution No Derivatives (CC BY-ND)\",\"element_set\":{\"id\":\"1\"},\"html\":true},"
                + "{\"element\":{\"id\":\"109\"},\"text\":\"Van Leuven, Oude tuinbouwvoorwerpen, 1995.\",\"element_set\":{\"id\":\"1\"},\"html\":true}"
                + "],"
                + "\"item_type\":{\"id\":\"6\"},\"modified\":null}";
            
            boolean expResult = true;
        try {
            JSONObject omekaObject = (JSONObject)new JSONParser().parse(strItem);
            boolean result = this.omekaRecords.addItem(omekaObject, "item");
            assertEquals(expResult, result);     
            
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
        } 
    }
    
    /**
     * Test of addItem method, of class OmekaData.
     * Test if a dmt returned record is correctly processed into an omeka compliant
     * item.
     */    
    public void testprocessRecords(){          
        System.out.println("addItem");
               
        String strItem = "{"
                + "\"public\":\"true\",\"featured\":\"true\",\"added\":null,\"modified\":null,\"item_type\":\"Object\",\"collection\":null,\"tags\":\"zuivel; marketing; tentoonstelling\","
                + "\"element_texts\":[{\"html\":true,\"text\":\"00000012\",\"element_set\":null,\"element\":\"items::dc:Identifier\"},{\"html\":true,\"text\":\"34\",\"element_set\":null,\"element\":\"items::object:Object_id\"},{\"html\":true,\"text\":\"foto\",\"element_set\":null,\"element\":\"items::object:Objectnaam\"},{\"html\":true,\"text\":\"Foto van de nationale propaganda voor zuivelproducten uit 1935\",\"element_set\":null,\"element\":\"items::dc:Title\"},{\"html\":true,\"text\":\"Deze zwart\\/witfoto toont een aantal affiches die gebruikt werden ter promotie van zuivelproducten op een landbouwtentoonstelling in Belgi\\u00eb rond 1935. Deze foto maakt deel uit van een serie foto's over landbouwtentoonstellingen in Belgi\\u00eb rond 1935.\",\"element_set\":null,\"element\":\"items::dc:Description\"},{\"html\":true,\"text\":\"KADOC - KU Leuven\",\"element_set\":null,\"element\":\"items::dc:Provenance\"},{\"html\":true,\"text\":\"Archief Boerenbond\",\"element_set\":null,\"element\":\"items::object:Collectie\"},{\"html\":true,\"text\":\"Boerenbond\",\"element_set\":null,\"element\":\"items::dc:Creator\"},{\"html\":true,\"text\":\"Leuven - 3000 \",\"element_set\":null,\"element\":\"items::dc:Spatial coverage\"},{\"html\":true,\"text\":\"1930 - 1940\",\"element_set\":null,\"element\":\"items::dc:Date\"},{\"html\":true,\"text\":\"843283\",\"element_set\":null,\"element\":\"items::object:digitoolurl\"},{\"html\":true,\"text\":\"842869\",\"element_set\":null,\"element\":\"items::object:digitoolurl\"},{\"html\":true,\"text\":\"Attribution No Derivatives (CC BY-ND)\",\"element_set\":null,\"element\":\"items::dc:License\"},{\"html\":true,\"text\":\"Van Leuven, Oude tuinbouwvoorwerpen, 1995.\",\"element_set\":null,\"element\":\"items::dc:References\"},{\"html\":true,\"text\":[{\"georeference\":{\"latitude\":\"50.8796\",\"longitude\":\"4.7009\",\"path\":\"50.8796,4.7009\",\"label\":\"Leuven - 3000 \"}},{\"georeference\":{\"latitude\":\"50.8466\",\"longitude\":\"4.3528\",\"path\":\"50.8466,4.3528\",\"label\":\"Bruxelles - 1000 \"}}],\"element_set\":null,\"element\":\"items::geolocation:address\"},{\"html\":true,\"text\":\"items\",\"element_set\":null,\"element\":\"Resource\"}]"
                + "}";
            
        try {
            this.omekaRecords.omekaElements = this.omekaRecords.getTypesElements(); 
            JSONObject omekaObject = (JSONObject)new JSONParser().parse(strItem);
            
            /* Process record method changes a record received from the dmt service to omeka compliant. The outcome of this method
            can be used for adding/updating the corresponding record in omeka. */
            List responeList = this.omekaRecords.processRecords(omekaObject, "6", "objecten"); // type_id = 6
                        
            assertNotNull(responeList);
            assertEquals(2, responeList.size());
            
            boolean validOperationType = false;
            if(responeList.get(0).toString().equals("UPDATE") || responeList.get(0).toString().equals("ADD"))
                validOperationType = true;
            assertEquals(true, validOperationType); 

        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
        }
    }    
    
}
