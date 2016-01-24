/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package example;


import com.rabbitmq.client.AMQP;

import org.apache.commons.codec.binary.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.soap.SOAPException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

import javax.xml.soap.SOAPConnection;
import javax.xml.soap.SOAPConnectionFactory;
import javax.xml.soap.SOAPMessage;

import org.json.simple.parser.ParseException;
import org.w3c.dom.NodeList;

/**
 *
 * @author Eliott
 */
public class SoapConsumer {

    private String id;
    private String exchange;
    private String broadcast;
    private String callback;
    private Connection queueConnection;
    private Channel channel;
    private SOAPConnection soapConnection;
    private JSONParser parser;
    private int startingTime;
    private int size;
    private int duration;
    private int period;
    private String provider;
    private Boolean endReceived;
    private JSONObject data;
    private JSONArray listSent;
    private JSONArray listReceived;
    private int cptFails;

    @SuppressWarnings("unchecked")
	public SoapConsumer(String id, String exchange, String broadcast, String callback)
    {
        this.id = id;
        this.exchange = exchange;
        this.broadcast = broadcast;
        this.callback = callback;
        this.queueConnection = null;
        this.channel = null;
        this.soapConnection = null;
        this.parser = new JSONParser();
        this.startingTime = 0;
        this.size = 1;
        this.duration = 0;
        this.period = 0;
        this.provider = null;
        this.endReceived = false;
        this.data = new JSONObject();
        this.data.put("id", this.id);
        this.listSent = new JSONArray();
        this.listReceived = new JSONArray();
        this.cptFails = 0;
    }

    public void run () throws Exception
    {
        //new rabbitMQ connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        this.queueConnection = factory.newConnection();
        this.channel = queueConnection.createChannel();

        // declare the exchange
        this.channel.exchangeDeclare(this.exchange, "direct");

        //declare the queue
        String queueName = channel.queueDeclare().getQueue();

        //bind the queue to the exchange (id + broadcast);
        this.channel.queueBind(queueName, this.exchange, this.id);
        this.channel.queueBind(queueName, this.exchange, this.broadcast);

        //wait for orders message
        System.out.println(" [*] Waiting for orders.");

        //create new consumer
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            //Executed when a message is received
            public void handleDelivery(String consumerTag, Envelope envelope,
                                     AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
                JSONObject obj;
                try {
                    obj = (JSONObject) parser.parse(message);
                    if (obj.containsKey("type")) {
                        String type = (String) obj.get("type");
                        //if config message
                        if(type.equals("config")) {
                            if (obj.containsKey("startingTime"))
                                setStartingTime(Integer.parseInt((String) obj.get("startingTime")));
                            if (obj.containsKey("size"))
                                setSize(Integer.parseInt((String) obj.get("size")));
                            if (obj.containsKey("duration"))
                                setDuration(Integer.parseInt((String) obj.get("duration")));
                            if (obj.containsKey("period"))
                                setPeriod(Integer.parseInt((String) obj.get("period")));
                            if (obj.containsKey("provider"))
                                setProvider((String) obj.get("provider"));
                        } else {
                        	//if go message
                            if(type.equals("go")) {
                                try {
                                    if(getProvider() == null) {
                                        putData("error", "provider unknown");
                                        disconnectEverything();
                                    } else {
                                        doRequest();
                                    }
                                } catch (SOAPException ex) {
                                    Logger.getLogger(SoapConsumer.class.getName()).log(Level.SEVERE, null, ex);
                                    putData("error", "SOAPException");
                                } catch (InterruptedException ex) {
                                    Logger.getLogger(SoapConsumer.class.getName()).log(Level.SEVERE, null, ex);
                                    putData("error", "InterruptedException");
                                } catch (Exception ex) {
                                    Logger.getLogger(SoapConsumer.class.getName()).log(Level.SEVERE, null, ex);
                                    putData("error", "Exception");
                                }
                            } else {
                            	//if end message
                            	if(type.equals("end")) {
                            		if(getProvider() == null) {
                                        disconnectEverything();
                            		} else {
                            			setEndReceived(true);
                                    }
                            	} else {
                            		System.out.println("[!] Last message was unreadable");
                            	}
                            }
                        }
                    } else {
                        System.out.println("[!] Last message was unreadable");
                    }
                } catch (ParseException ex) {
                        Logger.getLogger(SoapConsumer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };

        this.channel.basicConsume(queueName, true, consumer);
    }

    //Send requests to provider and store data
    @SuppressWarnings("unchecked") //http://stackoverflow.com/questions/2646613/how-to-avoid-eclipse-warnings-when-using-legacy-code-without-generics
	private void doRequest() throws SOAPException, InterruptedException, Exception {
	time3 = System.currentTimeMillis();
        time4 = System.currentTimeMillis();
        
        int cpt = 0;
        long time1, time2, time3, time4;

        //new soap connection
        SOAPConnectionFactory soapFactory = SOAPConnectionFactory.newInstance();
        this.soapConnection = soapFactory.createConnection();

        //sleep waiting for the starting time
        System.out.println("Starting time : Sleep for " + this.startingTime + " ms\n");
        Thread.sleep(startingTime);

        

        while(time4 + this.duration + this.startingTime > time3 && !this.endReceived) {
            // Send SOAP Message to SOAP Web Service
            time1 = System.currentTimeMillis();
            SOAPMessage soapResponse = soapConnection.call(SoapMessageCreator.createSOAPRequest(), this.provider);
            time2 = System.currentTimeMillis();
            JSONObject sent = new JSONObject();
            sent.put("id", this.id + "-" + cpt);
            sent.put("time", String.valueOf(time1 - time4));
            JSONObject received = new JSONObject();
            received.put("id", this.id + "-" + cpt);

            // Process the SOAP Response
            SoapMessageCreator.printSOAPResponse(soapResponse);
            NodeList listReturn = soapResponse.getSOAPBody().getElementsByTagName("return");
            if(listReturn.getLength() != 0) {
                Base64 decoder = new Base64();
            	byte[] result = decoder.decode(listReturn.item(0).getTextContent());
            	//byte[] result = listReturn.item(0).getTextContent().getBytes("UTF-8");
            	System.out.println("\nNumber of bytes received : " + result.length);
                received.put("time", String.valueOf(time2 - time4));

            } else {
            	System.out.println("\nThe request returned a fault");
            	this.cptFails++;
            	received.put("time", "-1");
            	received.put("error", soapResponse.getSOAPBody().getTextContent());
            }
            this.listSent.add(sent);
            this.listReceived.add(received);
            System.out.println("Delay of call : " + (time2 - time1) + " ms");
            System.out.println("\nPeriod : Sleep for " + period + " ms");
            Thread.sleep(this.period);
            cpt++;
            time3 = System.currentTimeMillis();
        }

        System.out.println("\nMission executed : " + cpt + " requests in " + (time3 - time4) + " ms ==> " + cptFails + " fails\n");
        doCallback();
    }

    //Send data to callback queue& disconnect
    @SuppressWarnings("unchecked")
	private void doCallback() throws IOException {
        this.data.put("errors", String.valueOf(cptFails));
        this.data.put("sent", listSent);
        this.data.put("received", listReceived);
        this.channel.queueDeclare(this.callback, false, false, false, null);
        channel.basicPublish("", this.callback, null, this.data.toJSONString().getBytes());
        this.disconnectEverything();
    }

    //Disconnect soapConnection and queueConnection
    private void disconnectEverything() {
        try {
        	System.out.println("\nDisconnecting soap & rabbitmq");
        	if(this.soapConnection != null)
        		this.soapConnection.close();
        	if(this.queueConnection != null)
        		this.queueConnection.close();
        } catch (IOException ex) {
            Logger.getLogger(SoapConsumer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SOAPException ex) {
            Logger.getLogger(SoapConsumer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    //Main function
    public static void main(String[] args)
    {
        try {
            SoapConsumer test = new SoapConsumer(args[0], args[1], args[2], args[3]);
            test.run();
        } catch (Exception ex) {
            Logger.getLogger(SoapConsumer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public float getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getStartingTime() {
        return startingTime;
    }

    public void setStartingTime(int startingTime) {
        this.startingTime = startingTime;
    }

    @SuppressWarnings("unchecked")
	public void putData(String key, Object value) {
        this.data.put(key, value);
    }

    public void setEndReceived(Boolean endReceived) {
        this.endReceived = endReceived;
    }
}
