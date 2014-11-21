package libisinworker;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject; 
import org.json.simple.parser.JSONParser; 
import org.json.simple.parser.ParseException;

/**
 *
 * @author NaeemM
 */
public class Libisinworker {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ParseException, URISyntaxException {    
        
        LibisinUtil libisinUtils = new LibisinUtil();
        Logger serverLog = libisinUtils.createLogger(null, Libisinworker.class.getName());
        
                        
        Libisinworker worker = new Libisinworker();
        Properties queuingServerConfig = worker.getConfigurations("queuingserver", serverLog);
        Properties caServerConfig = worker.getConfigurations("caserver", serverLog);
        Properties dmtServiceConfig = worker.getConfigurations("dmtservice", serverLog);
        Properties omekaServiceConfig = worker.getConfigurations("omekaserver", serverLog);
        Properties libisinWorkerConfig = worker.getConfigurations("libisinworker", serverLog);
                        
        Connection connection = worker.connectQueuingServer(queuingServerConfig, serverLog);
        Channel channel = connection.createChannel();        
        channel.queueDeclare(queuingServerConfig.getProperty("rmq_queue_name"), false, false, false, null);
                
        QueueingConsumer consumer = new QueueingConsumer(channel);        
        channel.basicConsume(queuingServerConfig.getProperty("rmq_queue_name"), true, consumer);    

        String connectionMessage = "Connection established to: \n" 
            + "---------  \n"
            + "server: " + queuingServerConfig.getProperty("rmq_server") + " \n"
            + "port: " + queuingServerConfig.getProperty("rmq_port") + " \n"
            + "Queue: " + queuingServerConfig.getProperty("rmq_queue_name") + " \n"
            + "Consumer tag: " + consumer.getConsumerTag() + " \n"
            + "Startup time: " + (new Date()) + " \n"
            + "user: " + queuingServerConfig.getProperty("rmq_id") + " \n"
            + "---------  \n";
        
        System.out.println(connectionMessage);        
        serverLog.log(Level.INFO, connectionMessage);
        System.out.println("[*] Waiting for messages. To exit press CTRL+C"); 
        
        
        JSONParser parser = new JSONParser();
        Cadata setData = new Cadata();
        DmtService dmtService = new DmtService();
        OmekaData omekaRecords = new OmekaData(omekaServiceConfig);
                        
        while (true) {            
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            
            String requestDirectory = libisinUtils.createRequestDirectory();
            if(requestDirectory == null){
                System.out.println("Error in creating request directory. Stopping further processing of this request.");
                serverLog.log(Level.INFO, "Error in creating request directory: {0}, request will not be processed", requestDirectory);
                continue;
            }            
                       
            Logger requestLog = libisinUtils.createLogger(requestDirectory + "/request.log", setData.getClass().getName());
            omekaRecords.requestLog = requestLog;
            dmtService.requestLog = requestLog;
            setData.requestLog = requestLog;

            System.out.println("+---------------------------");
            System.out.println("Request processing started at: " + new Timestamp(new java.util.Date().getTime()));
            System.out.println("Request directory: " + new File(requestDirectory).getName());
            serverLog.log(Level.INFO, "Request processing started at: {0}", new Timestamp(new java.util.Date().getTime()));
            serverLog.log(Level.INFO, "Request directory created: {0}", requestDirectory);
            
            String message = new String(delivery.getBody());   
            JSONObject messageBodyobj = (JSONObject) parser.parse(message);             
            JSONObject userObj = (JSONObject) parser.parse(messageBodyobj.get("user_info").toString());
            String setInfoBody = messageBodyobj.get("set_info").toString();
            
            serverLog.log(Level.INFO, "Request info: {0}", setInfoBody);
            serverLog.log(Level.INFO, "User info: {0}", userObj);
            
            System.out.println("Processing Request of user: "+ userObj.get("name").toString());
            JSONArray setInfoBodyArray = (JSONArray) messageBodyobj.get("set_info");
            for(int i=0; i< setInfoBodyArray.size(); i++){
                
                //stop firewall                
                //libisinUtils.executeCommand("sudo /etc/init.d/firewall stop", serverLog);
                                
                JSONObject object = (JSONObject)setInfoBodyArray.get(i);                                                
                String mappingRules = object.get("mapping").toString();                              
                String mappingFilePath = libisinUtils.writeFile(requestDirectory + "/mappingrules.csv", mappingRules);
                serverLog.log(Level.INFO, "Mapping file: {0}", mappingFilePath);
                                
                ////retrieve records from collectiveaccess
                String caRecordsFilePath = setData.getSetData(object.get("set_name").toString(),
                        caServerConfig, object.get("bundle").toString(), object.get("record_type").toString(), requestDirectory); 
                
                if(caRecordsFilePath == null){
                    System.out.println("Error in receiving data from Collective Access.");
                    serverLog.log(Level.SEVERE, "Error in receiving data from Collective Access");
                    requestLog.log(Level.SEVERE, "Error in receiving data from Collective Access");
                    return;
                }
                requestLog.log(Level.INFO, "Collective Access records file: {0}", caRecordsFilePath);

                ////send records to mapping service to map to omeka format               
                String dmtMapResponse = dmtService.mapData(caRecordsFilePath, mappingFilePath, dmtServiceConfig);
                
                JSONObject requestIdobj = (JSONObject) parser.parse(dmtMapResponse);
                String dmtRequestId = requestIdobj.get("request_id").toString();                
                                                
                requestLog.log(Level.INFO, "DMT mapping request id: {0}", dmtRequestId);
                            
                ////fetch mapping result
                String omekaData = dmtService.fetchOmekaDmt(dmtRequestId, dmtServiceConfig);      
                                                
                if(omekaData.length() > 0) {
                    omekaRecords.pushDataToOmeka(omekaData, object.get("record_type").toString(), requestDirectory, object.get("set_name").toString());  
                    requestLog.log(Level.INFO, "DMT fetch successfull. Length of omeka data to add/update: {0} characters", omekaData.length());
                }                    
                else{
                    System.out.println("No data to add/update in omeka.");
                    serverLog.log(Level.SEVERE, "no data to add/update in omeka");
                    requestLog.log(Level.SEVERE, "no data to add/update in omeka");
                }                                                                                                                   
            }
            System.out.println("Request processing finished at: " + new Timestamp(new java.util.Date().getTime()));
            libisinUtils.sendEmail(libisinWorkerConfig, userObj.get("email").toString(), userObj.get("name").toString(), requestDirectory+ "/request.log", requestLog);
            System.out.println("---------------------------+\n");
            libisinUtils.destroyLogger(requestLog);                                          
        }        

    }
    
    public Connection connectQueuingServer(Properties queuingServerConfig, Logger serverLog){        
        ConnectionFactory factory = new ConnectionFactory();        
        factory.setHost(queuingServerConfig.getProperty("rmq_server"));
        factory.setPort(Integer.parseInt(queuingServerConfig.getProperty("rmq_port")));
        factory.setUsername(queuingServerConfig.getProperty("rmq_id"));
        factory.setPassword(queuingServerConfig.getProperty("rmq_pwd"));
        factory.setVirtualHost(queuingServerConfig.getProperty("rmq_vhost"));
        //factory.setVirtualHost("/");                    //temporary, remove it for production
        
        Connection connection = null;
        try {
            connection = factory.newConnection();
        } catch (IOException ex) {
            serverLog.log(Level.SEVERE, "Error in connecting queuing server: {0}", queuingServerConfig.getProperty("rmq_server"));
            serverLog.log(Level.SEVERE, "Exception Message: {0}", ex.getMessage());
            System.out.println("Error in connecting queuing server: " + queuingServerConfig.getProperty("rmq_server"));
            System.exit(-1);
        }
        return connection;
    }
    
    public Properties getConfigurations(String configuratinFor, Logger serverLog){
        Properties prop = new Properties();
        String configFile = "";
        switch(configuratinFor){
            case "queuingserver":
                configFile = "queue_server_conf";
                break;
                
            case "caserver":
                configFile = "ca_server_conf";
                break;                
                
            case "dmtservice":
                configFile = "dmt_service_conf";
                break;                 
                
            case "omekaserver":
                configFile = "omeka_server_conf";
                break;   

            case "libisinworker":
                configFile = "libisinworker_conf";
                break; 
        }
        
        try {	                         
            prop.load(this.getClass().getResourceAsStream("/resources/" + configFile + ".properties"));                       						
        } catch (IOException | NullPointerException ex) {            
            serverLog.log(Level.SEVERE, "Error in reading configuration file for: {0}", configuratinFor);
            serverLog.log(Level.SEVERE, "File '{0}' not found", configFile);
            serverLog.log(Level.SEVERE, "Exception Message: {0}", ex.getMessage());
            System.out.println("Configuration file not found. Quitting.");
            System.exit(-1);
        } 		  
        return prop; 		  
    }    
    
}
