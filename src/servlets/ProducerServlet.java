package servlets; 

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;



//import util.properties packages
import java.util.Properties;


//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord; 

/**
 * Servlet implementation class ProducerServlet
 */
@WebServlet("/test")
public class ProducerServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public ProducerServlet() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
       
		String topic = request.getParameter("topic");
        String msg = request.getParameter("msg");


     	// String topicName =topic;
         // create instance for properties to access producer configs   
         Properties props = new Properties();
         
         //Assign localhost id
         props.put("bootstrap.servers", "localhost:9092");
         
         //Set acknowledgements for producer requests.      
         props.put("acks", "all");
         
         //If the request fails, the producer can automatically retry,
         props.put("retries", 0);
         
         //Specify buffer size in config
         props.put("batch.size", 16384);
         
         //Reduce the no of requests less than 0   
         props.put("linger.ms", 1);
         
         //The buffer.memory controls the total amount of memory available to the producer for buffering.   
         props.put("buffer.memory", 33554432);
         
         props.put("key.serializer", 
            "org.apache.kafka.common.serialization.StringSerializer");
            
         props.put("value.serializer", 
            "org.apache.kafka.common.serialization.StringSerializer");
         
         Producer<String, String> producer = new KafkaProducer
            <String, String>(props); 
         
         //send the message to a topic 
         producer.send(new ProducerRecord<String, String>(topic,msg ));
         
	         PrintWriter out = response.getWriter();
	         out.println("<html>");
	         out.println("<head><title>Write to topic: "+ topic +"</title></head>");
	         out.println("<body> message <b> "+ msg +" <b>has been sent to topic:<b> "+ topic +" </h1> ");
	         out.println("</html>");
	         out.close();
         
           producer.close();
	

	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

}
