package example;

//import java.io.UnsupportedEncodingException;

import javax.xml.soap.MessageFactory;
import javax.xml.soap.Name;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPMessage;
import javax.xml.soap.SOAPPart;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;

/**
 *
 * @author Eliott
 */
public class SoapMessageCreator {

        public static SOAPMessage createSOAPRequest() throws Exception {
        MessageFactory messageFactory = MessageFactory.newInstance();
        SOAPMessage soapMessage = messageFactory.createMessage();
        SOAPPart soapPart = soapMessage.getSOAPPart();

        // SOAP Envelope
        SOAPEnvelope envelope = soapPart.getEnvelope();

        /*
        Constructed SOAP Request Message:
        <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
            <SOAP-ENV:Header/>
            <SOAP-ENV:Body>
                <ns2:doStuff xmlns:ns2="http://example/"/>
            </SOAP-ENV:Body>
        </SOAP-ENV:Envelope>
         */

        // SOAP Body
        SOAPBody soapBody = envelope.getBody();
        Name bodyName = envelope.createName("doStuff", "ns2","http://example/");
        soapBody.addBodyElement(bodyName);

        soapMessage.saveChanges();

        /* Print the request message */
        System.out.print("\nRequest SOAP Message = ");
        soapMessage.writeTo(System.out);

        return soapMessage;
    }

    /**
     * Method used to print the SOAP Response
     */
    public static void printSOAPResponse(SOAPMessage soapResponse) throws Exception {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        Source sourceContent = soapResponse.getSOAPPart().getContent();
        System.out.print("\nResponse SOAP Message = ");
        StreamResult result = new StreamResult(System.out);
        transformer.transform(sourceContent, result);
    }

    /*private String generateMessage(int messageSize) throws UnsupportedEncodingException {
        byte[] mess = new byte[messageSize];
        for(int i=0;i < messageSize;i++) {
            mess[i] =(byte) ('a' +(i%26));
        }
        
        return (new String(mess, "UTF_8"));
    }*/

}
