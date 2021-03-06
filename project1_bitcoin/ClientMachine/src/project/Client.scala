package project


import akka.actor._
import akka.routing._
import java.security.MessageDigest
import common._


case class 	MineBitcoinsActor(start : Int, nrofElements: Int)
case class 	Message (msg : String)
case class 	MineBitcoinsWithinRange(start : Int, nrofElements: Int, pattern : StringBuffer)
case object MineBitcoins

    
object Client{
  
  def main(args: Array[String]){
	  println("****************************************************************************************")
	   
	   implicit val system = ActorSystem("LocalSystem")
	   val bossActor = system.actorOf(Props(new ClientMaster (args(0))), name = "ClientMaster")
	   //below line for hard coded IP address
	   //val bossActor = system.actorOf(Props[ClientMaster], name = "ClientMaster")
	  System.setProperty("java.net.preferIPv4Stack", "true")
	   println("****************************************************************************************")
	   bossActor  ! Message("Remote Workers are available")
   }
}

class ClientMaster (ip: String) extends Actor {
//class ClientMaster extends Actor {

	val remote = context.actorFor("akka.tcp://WorkerSystem@"+ip+":5570/user/ServerMaster")
	//below line for hard coded IP
    //val remote = context.actorSelection("akka.tcp://WorkerSystem@192.168.0.29:5570/user/ServerMaster")
	val clientworker = context.actorOf(Props[ClientWorker].withRouter(RoundRobinRouter(4)), name = "ClientWorkers")
	var counter = 0 
	var i = 0
  

  def receive = {
	  
   case ServerToClientReq (start, nrOfElements, pattern) =>
     	
     	println ("Got Request Frome sever " + start + "Numbe of Elements " + nrOfElements + " Pattern to find " + pattern )
     	
   		 var begin = start
      	 clientworker ! MineBitcoinsWithinRange(begin, nrOfElements/4, pattern)
	     begin = begin + nrOfElements/4
	     clientworker ! MineBitcoinsWithinRange(begin, nrOfElements/4, pattern)
	     begin = begin + nrOfElements/4
	     clientworker ! MineBitcoinsWithinRange(begin, nrOfElements/4, pattern)
	     begin = begin + nrOfElements/4
	     clientworker ! MineBitcoinsWithinRange(begin, nrOfElements/4, pattern)
	     begin = begin + nrOfElements/4
	     //remote ! "Remote Workers available"
        
   	case Message(msg) =>
         remote ! ClientToServer (msg)
         
   	case ServerToClient(msg) =>
   	     println(s"Got Message from Server '$msg'")
   	     
   	//case ServerToClientReq (start, )
   	case ClientToServerResp(msg)   =>
   	      	println ("Client Worker Bit coins")
    	println (msg)
   	  remote ! ClientToServerResp (msg)
        
  }
}

class ClientWorker extends Actor {  
 def receive = {
    case MineBitcoinsWithinRange (start, nrOfElements, pattern) =>
    	val hg = new HashGenerator ()
    	var outputBitCoin: String = hg.GetHash(start, start + nrOfElements, pattern)  

    	sender ! ClientToServerResp (outputBitCoin)      
        
    }
  
}

class HashGenerator { 
	
	def GetHash(countStart: Int, countEnd : Int, pattern : StringBuffer) : String = {
    
		val md = MessageDigest.getInstance("SHA-256");
		var i = 0
		var outputBitCoin: String = ""  
  	    
		//println ("Number of Leading Zeros Requird " + numOfLeadingZeros)
  	  	for (i <- countStart until countEnd){ 
  	    	
  	  		
  	  		val mdbytes = md.digest(("rkmalik" + i.toString()).getBytes);
  	  		val sb = new StringBuffer();
  	  		//var currentBitCoin = HexOfBuffer(mdbytes)
  	  		sb.append(HexOfBuffer(mdbytes))  	  		
  	  		
  	  		if (sb.indexOf(pattern.toString()) == 0){
  	  		  
  	  			//println ("GetHash : Using Index of Zeros " + sb.toString())
  	  			outputBitCoin = outputBitCoin  + "\n" + ("rkmalik" + i.toString()) + "\t" + sb.toString(); 	  		  
  	  		}
	  			
  	  	}		
		
		// Output the Payload of Bit Coins mined to 
		outputBitCoin
  }
  
  // Definition of new Method HexOfBuffer X is used to get hex code in capital letters while 
  // x (small x is used to get the hex value in small letters
  def HexOfBuffer(buf: Array[Byte]): String = {
    buf.map("%02x" format _).mkString
  }  
}

