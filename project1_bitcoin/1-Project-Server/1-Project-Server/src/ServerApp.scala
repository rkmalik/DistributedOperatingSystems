//package server

import akka.actor._
import akka.routing._
import java.security.MessageDigest
//import messages._

import common._


object ServerApp{
  
  def main(args: Array[String]){
    
	  val numOfZeros = args (0).toInt	  
	  val pattern = new BuildPattern
	  val system = ActorSystem("WorkerSystem")
	  val ServerMaster = system.actorOf(Props(new ServerMaster(pattern.getPattern(numOfZeros))), name = "ServerMaster")
	  System.setProperty("java.net.preferIPv4Stack", "true")
	  ServerMaster ! MineBitcoins
	  ServerMaster ! MineBitcoins
	  ServerMaster ! MineBitcoins
	  ServerMaster ! MineBitcoins
	  

  }
}

class BuildPattern {
  
  def getPattern (numOfZeros : Int) : StringBuffer = {
    
    // Develop starting pattern 
	  val i = 0
	  //var pattern = new StringBuffer ()
	  var pattern = new StringBuffer ()

	  for (i <- 0 until numOfZeros) {
		  pattern.append("0".toString ())
	  }
	  pattern
  }  
}

// This class takes the number or zeros to be mined 
class ServerMaster (pattern: StringBuffer) extends Actor {
  

	var start = 0
	var nrOfElements = 200000
	var remoteElements = nrOfElements *4
	val serverworker = context.actorOf(Props (new ServerWorker).withRouter(RoundRobinRouter(4)), name = "LocalWorkers")
	 
	def receive = {
				
	  	case msg: String => 		  	
		//case msg =>
			println(s"Server Workers Minded the BitCoins'$msg'")
	  	  	//println(msg)
			var end : Int= start +nrOfElements
			println ("Start " + start + "End " + end)
			sender ! MineBitcoinsWithinRange(start, nrOfElements, pattern)
			start = start + nrOfElements
	  
		case MineBitcoins =>
			serverworker ! MineBitcoinsWithinRange(start, nrOfElements, pattern)
	        start = start + nrOfElements
	        println ("ServerMaster:MineBitcoins")
		
		case ClientToServer(msg) =>

			println (s"Got a message from Client'$msg'")
			var end : Int= start +remoteElements
			println ("Start " + start + "   End " + end)
			sender ! ServerToClientReq (start, remoteElements, pattern)
			
		case ClientToServerResp (msg) =>
			println (s"Remote Worker Mined the BitCoins'$msg'")

		
  	}
}

// This the worker actor takes the number of Zeros to be found in the given range and return the 
// output payload of the mined bit coins
class ServerWorker extends Actor {
  
	def receive = {
    	case MineBitcoinsWithinRange (start, nrOfElements, pattern) =>
    	  
    		val hg = new HashGenerator ()
    		var outputBitCoin: String = hg.GetHash(start, start + nrOfElements, pattern)
    		
     		//println (outputBitCoin)
    		sender ! outputBitCoin    	
  }  
}


class HashGenerator { 
	
	def GetHash(countStart: Int, countEnd : Int, pattern : StringBuffer) : String = {
    
		val md = MessageDigest.getInstance("SHA-256");
		var i = 0
		var outputBitCoin: String = ""  
  	    
		//println ("Number of Leading Zeros Requird " + numOfLeadingZeros)
		//println ("Start " + countStart + "End " + countEnd)
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

