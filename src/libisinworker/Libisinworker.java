package libisinworker;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
//import org.json.simple.JSONArray;
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
    public static void main(String[] args) throws IOException, InterruptedException, ParseException {    
        
        
        Libisinworker worker = new Libisinworker();
        Properties queuingServerConfig = worker.getConfigurations("queuingserver");
        Properties caServerConfig = worker.getConfigurations("caserver");
        Properties dmtServiceConfig = worker.getConfigurations("dmtservice");
        Properties omekaServiceConfig = worker.getConfigurations("omekaserver");
                
        Connection connection = worker.connectQueuingServer(queuingServerConfig);
        Channel channel = connection.createChannel();        
        channel.queueDeclare(queuingServerConfig.getProperty("rmq_queue_name"), false, false, false, null);
        
        System.out.println("Connection established to: " 
        + queuingServerConfig.getProperty("rmq_server") 
        + " at port: " + queuingServerConfig.getProperty("rmq_port") 
        + ", for user: " + queuingServerConfig.getProperty("rmq_id"));
         
        QueueingConsumer consumer = new QueueingConsumer(channel);
        
        channel.basicConsume(queuingServerConfig.getProperty("rmq_queue_name"), true, consumer);    
        System.out.println("Queue: " + queuingServerConfig.getProperty("rmq_queue_name"));
        System.out.println("Consumer tag: " + consumer.getConsumerTag());
        System.out.println("Start timestamp: " + (new Date()));
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");   
        
        JSONParser parser = new JSONParser();
        Cadata setData = new Cadata();
        DmtService dmtService = new DmtService();
        OmekaData omekaRecords = new OmekaData(omekaServiceConfig);
        LibisinUtil libisinUtils = new LibisinUtil();

        
        while (true) {            
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());   
            JSONObject messageBodyobj = (JSONObject) parser.parse(message);             
            JSONObject userObj = (JSONObject) parser.parse(messageBodyobj.get("user_info").toString());
            String setInfoBody = messageBodyobj.get("set_info").toString();
            
            JSONArray setInfoBodyArray = (JSONArray) messageBodyobj.get("set_info");
            for(int i=0; i< setInfoBodyArray.size(); i++){                
                JSONObject object = (JSONObject)setInfoBodyArray.get(i);
                
                String mapping = object.get("mapping").toString();                              
                String mappingFilePath = libisinUtils.writeTempFile("temp", "mappingrules.csv", mapping);

                ////retrieve records from collectiveaccess
                String tempFilePath = setData.getSetData(object.get("set_name").toString(),
                        caServerConfig, object.get("bundle").toString()); 

                ////send records to mapping service to map to omeka format               
                String dmtMapResponse = dmtService.mapData(tempFilePath, mappingFilePath, dmtServiceConfig);                
                JSONObject requestIdobj = (JSONObject) parser.parse(dmtMapResponse);
                String dmtRequestId = requestIdobj.get("request_id").toString();
				
                ////fetch mapping result
                String omekaData = dmtService.fetchOmekaDmt(dmtRequestId, dmtServiceConfig);    
				
                ////update records in omeka
                omekaRecords.updateData(omekaData);   
            }
        }
        
    }
    
    public Connection connectQueuingServer(Properties queuingServerConfig){        
        ConnectionFactory factory = new ConnectionFactory();        
        factory.setHost(queuingServerConfig.getProperty("rmq_server"));
        factory.setPort(Integer.parseInt(queuingServerConfig.getProperty("rmq_port")));
        factory.setUsername(queuingServerConfig.getProperty("rmq_id"));
        factory.setPassword(queuingServerConfig.getProperty("rmq_pwd"));
        factory.setVirtualHost(queuingServerConfig.getProperty("rmq_vhost"));
        
        Connection connection = null;
        try {
            connection = factory.newConnection();
        } catch (IOException ex) {
            Logger.getLogger(Libisinworker.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println(ex.getMessage());
        }
        return connection;
    }
    
    public Properties getConfigurations(String configuratinFor){
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
        }
        
        try {	                         
            prop.load(this.getClass().getResourceAsStream("/resources/" + configFile + ".properties"));                       						
        } catch (IOException ex) {            
            System.out.println(ex.getMessage());            
        } 		  
        return prop; 		  
    }    
    
}
