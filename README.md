# SoapJavaConsumer

Wait for 4 arguments :
1. name of the unicast queue
2. name of the exchange
3. name of the broadcast queue
4. name of the callback queue (in order to return data)

Concerning the structure of the json files, the consumer is waiting for :

//config file to set all the paramaters for the sending of requests
{
  "type" : "config",
  "startingTime : "1000",   //Time to wait before the sending of requests (in ms)
  "size" : "10",            //Number of bytes to send to the service (not implemented yet)
  "duration" : "100000",    //Duration during which the consumer send requests to service (in ms)
  "period" : "20",          //Period between two succesives requests (in ms)
  "provider" : "http://localhost:8080/TestProducer1_war/SoapProviderService?wsdl", //Url of the wsdl of the service
}

//go file to launch the sending of requests (if config file not received, default paramaters are set)
{
  "type : "go"
}
