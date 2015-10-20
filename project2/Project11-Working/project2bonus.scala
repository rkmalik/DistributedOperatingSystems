package project2bonusworking

import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.Props
import akka.actor.ActorSystem
import scala.util.Random
import scala.concurrent.duration.Duration
import scala.collection.mutable.ArrayBuffer
import akka.dispatch.ExecutionContexts
import scala.concurrent.ExecutionContext
import com.sun.jna.platform
import scala.compat.Platform
import sun.reflect.MagicAccessorImpl
import akka.actor.Kill
import akka.actor.ActorRef

case object msg

object project2bonus {
  def main(args: Array[String]){
	  if(args.length != 3){
	    println("Invalid Parameters")
	  }
	  else {
	    val context = ActorSystem("Network")
		val master = context.actorOf(Props(new Master(args(0).toInt,args(1),args(2))),"masterbuddy")
	  }
	}
}

class NetworkNode extends Actor {

	import context._
	// Take the reference of the parent node while initializing the Node
	var parentNode:ActorRef = null
	var activeLines = ArrayBuffer[String]()
	var isPushSum:Boolean = false
    var deadNodes = ArrayBuffer [String] ()	
	var brdcstmsg:String = ""
	var msgcount = 0
	var staticCount = 0
	var diffcounter = 0
	var s=0.0; var w=1.0; var ratio = 0.0; var ratio_prev =999.0; var ratio_diff = 999.0; 
	  
	def BrodCastMessage {  
	  val msg = if(isPushSum == false)"rt"+brdcstmsg else "rf,"+s.toString+","+w.toString

	  var conlength =  activeLines.length
	  var random = Int.MaxValue
	  
	  while (random > conlength || random == 0) {
		  random = (Random.nextInt % conlength).abs
	  }

	  var targetNode = context.actorSelection("/user/Node"+ activeLines(random))
	  targetNode ! msg
	}
  
  def receive = {
    
    case msg:String => {
      //println (msg)
      msg.head match {
        
        case 'f' =>
          
          val msgtokens = msg.tail.split(",")
          val totalnodes = msgtokens(2)
          val curNodeIndex = msgtokens (1)   
          
          //println ("ROHIT - Active Lines")
          for (i <- 1 to totalnodes.toInt){            
            activeLines += i.toString            
          } 
          
          activeLines.update(0, curNodeIndex)
          activeLines.update((curNodeIndex.toInt - 1), 1.toString)
        
        case 'i' =>
          parentNode = sender
          
        case 'k'=> 
          parentNode ! "x"+activeLines(0)
          context.stop(self)
        case 'c'=> 
          deadNodes ++= msg.tail.split(",")

       
        // This is the line topology, we need to process this. 
        case 'l' =>          
          activeLines ++= msg.tail.split(",")         

          
        case 'r' =>
          msg.tail.head match {
            case 't'=> isPushSum = false
            case 'f'=> isPushSum = true
          }
          
          // Case when we are handling the gossip algorithm
          if (isPushSum == false && msgcount< 10) {
	          msgcount +=1
	          brdcstmsg = msg.tail.tail
	          
	          //if (activeLines.length >0) {
	          val sendmsg = "d"+activeLines(0);
        	  	parentNode ! sendmsg;
	          //}
	          BrodCastMessage
          }
                    //need re-transmission
		  if(isPushSum == false && msgcount <9) {
		    val dur = Duration.create(50, scala.concurrent.duration.MILLISECONDS);
		    val me = context.self
		    context.system.scheduler.scheduleOnce(dur, me, "s")
		    
		  } else if (msgcount == 10) {
        	  msgcount+=1;
        	  val sendmsg = "d"+activeLines(0);
        	  parentNode ! sendmsg;
        	  println(context.self +" is done.")
          } else if (isPushSum == true) {
            
              
            if (s == 0.0) {
            	s = activeLines(0).toDouble        		  
        	}
            
      
            var pushsumparams = msg.tail.tail.split(",")
        	s = (s+pushsumparams(1).toDouble)/2
        	w = (w+pushsumparams(2).toDouble)/2
        	ratio_prev = ratio
        	  ratio = s/w
        	  println(self+ratio.toString)
        	  ratio_diff = (ratio-ratio_prev).abs
        	  if(ratio_diff < 0.0000000001) diffcounter += 1
        	  else diffcounter = 0
        	  if(diffcounter == 3) {
        		  val sendmsg = "d"+activeLines(0)+","+ratio.toString;
        		  parentNode ! sendmsg;
        	}
        	  else if (diffcounter < 3) {
        		  BrodCastMessage
        	  }
        	  else if (diffcounter > 3) {
        		  println("Terminated actor is getting messages")
        	  }       	
            
          }
          
        // This scenario will spread the gossip.	
        case 'g' =>
          isPushSum == false
          brdcstmsg = msg.tail
          msgcount = msgcount + 1
          //println (brdcstmsg)
          
          // Send a done message to the parent node. 
          //Rohit:
          val msg_to_parent = "d" + activeLines(0)
          //Rohit: 
          parentNode ! msg_to_parent
          
          // This method will transmit the message to all the nodes.
          BrodCastMessage
          
        case 's'=>
          BrodCastMessage
          
        case 'p' => 
          
          isPushSum  = true
          s = activeLines(0).toDouble
          ratio = s/w
          ratio_diff = (ratio_prev-ratio).abs
          BrodCastMessage         
          
          
        case '2' =>
          activeLines ++= msg.tail.split(",")
          
        case 'm' =>
          activeLines ++= msg.tail.split(",")
          	
        case _ => 
          println ("This algorithm is not supported")
          
      }      
    }   
  }  
}

// This class will decide the topology and the algorithm to use. 
class Master (nodecount: Int, topology: String, algo: String) extends Actor {
	import context._
	var top = topology.toLowerCase()  
	var message:String = "init"
	var startTime:Long = 0
	var networkNodeState = Array.fill[Boolean](nodecount)(false)
	var deadnodescount = 5
	
	// get the node context
	val childContext = ActorSystem ("Network")
	
	for (i<- 1 to nodecount){		
		val node = childContext.actorOf(Props(new NetworkNode),"Node"+ i.toString)
		node ! message
    }
	// Reinitialize the message
	message = ""
  
	def Imperfect2DMsg {
	   val sidenodes = math.sqrt(nodecount).floor.toInt
	    // Convert the nodes in the perfect square
	    val totalnodes = math.pow(sidenodes, 2).toInt
	    // create all the nodes. 
	    for (i<- 1 to totalnodes) {
	      
	      var targetNode = childContext.actorSelection("/user/Node"+ i)
	      message = ""
	      message  = "m"+i.toString
	      
	      //Upper row check  
	      if(i-sidenodes > 0) {
	    	  message = message + "," + (i-sidenodes).toString 
	    	  //println ("i-sidenodes > 0" + message)
	      }
	      //lower row check
		  if(i+sidenodes <= totalnodes) {
			  message = message + "," + (i+sidenodes).toString  
			 //println ("i+sidenodes <= totalnodes" + message)
		  }
		  // right column check
		  if(i % sidenodes == 0) {
			  message = message + "," + (i-1).toString
			  //println ("i % sidenodes == 0" + message)			  
		  }		  
			// left column check
		  else if (i % sidenodes == 1) {
			  message = message + "," + (i+1).toString 
			  //println ("i % sidenodes == 1" + message)			  
		  }
		  // middle of the row
		  else {
		    	message = message + "," + (i-1).toString + "," + (i+1).toString 
		    	//println (message)		    	
		  }
		  
		  var random = Int.MaxValue
  		  while (random > totalnodes || random == 0) {
  			  random = (Random.nextInt % totalnodes).abs
  		  }

		  message =  message + "," + random.toString
		  targetNode ! message
	    }	  
	}
	
	def TwoDMsg {
	  
	  	val sidenodes = math.sqrt(nodecount).floor.toInt
	    // Convert the nodes in the perfect square
	    val totalnodes = math.pow(sidenodes, 2).toInt
	    // create all the nodes. 
	    for (i<- 1 to totalnodes) {
	      
	      var targetNode = childContext.actorSelection("/user/Node"+ i)
	      message = ""
	      message  = "2"+i.toString
	      
	      //Upper row check  
	      if(i-sidenodes > 0) {
	    	  message = message + "," + (i-sidenodes).toString 
	    	  //println ("i-sidenodes > 0" + message)
	      }
	      //lower row check
		  if(i+sidenodes <= totalnodes) {
			  message = message + "," + (i+sidenodes).toString  
			 //println ("i+sidenodes <= totalnodes" + message)
		  }
		  // right column check
		  if(i % sidenodes == 0) {
			  message = message + "," + (i-1).toString
			  //println ("i % sidenodes == 0" + message)			  
		  }		  
			// left column check
		  else if (i % sidenodes == 1) {
			  message = message + "," + (i+1).toString 
			  //println ("i % sidenodes == 1" + message)			  
		  }
		  // middle of the row
		  else {
		    	message = message + "," + (i-1).toString + "," + (i+1).toString 
		    	//println (message)		    	
		  }
		  
		  println(targetNode+" : "+message)
		  targetNode ! message
	    }
	  
	  
	}
	
	def LineMsg {
	  // Loop for all the actor nodes. and set the message in which I send the information for 
	    // the previous and the next node. 
	    // Loop for all the actor nodes. and set the message in which I send the information for 
	    // the previous and the next node. 
	    for (i<- 1 to nodecount) {
	      
	      var targetNode = childContext.actorSelection("/user/Node"+ i) 
	      var sendmsg:String = "l" + i.toString()

	      if (i>1) {        
	    	  sendmsg = sendmsg + "," + (i-1).toString()
	      } 
	      
	      if (i < nodecount) {
	        sendmsg = sendmsg + "," + (i+1).toString()        
	      }

	      targetNode ! sendmsg
	    }   
	}
	
	def FullMsg {
	  
	   // I send a message what is my node number and How many total Nodes are there in the network. 
	    for (i<- 1 to nodecount){
	    	message = ""
	    	// Lets Initialize this with f to find it in the NetworkNode actors
	    	message = "f" + "," + i.toString + "," + nodecount
	    	var targetNode = childContext.actorSelection("/user/Node"+ i)
	    	targetNode ! message    	  
	    } 
	  
	}	
	
	// Check for the topology and create it accordingly
	top	match {
	
	  // Here we create the nodes in the line topology. In this each actor knows about 
	  // its left and its right actor node. For this I can prepare a message and parse the message in the node
	  case "line" => 
	    LineMsg
	  // In this Every actor can talk to all other actors
	  case "full" =>
	    FullMsg
	  case "2d" => 
	    TwoDMsg	    
	  case "imperfect2d" => 
	    Imperfect2DMsg	   
	} 
	
	
	algo.toLowerCase() match {

		// prepare a message and send this to the random node.  
  		case "gossip" => 
  		  
  		  var msg:String = "g" + "Rohit is new CEO of facebook."
  		  //val rnd = new scala.util.Random
  		  //NOTE: ROHIT val range = nodecount*10 to nodecount*1000
  		  //NOTE: ROHIT var random:Int = rnd.nextInt(range.length)  	
  		  //NOTE: ROHIT Just check if this random generator has an issue with index. 
  		  var random = Int.MaxValue
  		  while (random > nodecount || random == 0) {
  			  random = (Random.nextInt % nodecount).abs
  		  }		  
  		  //println ("Created a random Number = " + random)
  		  var targetNode = childContext.actorSelection("/user/Node"+ random) 
  		  targetNode ! msg
  		  
  		case "push-sum" =>

  		  val msg:String="p"+0.0.toString + "," + 0.0.toString
  		  var random = Int.MaxValue
  		  while (random > nodecount || random == 0) {
  			  random = (Random.nextInt % nodecount).abs
  		  }
	    val targetChild = childContext.actorSelection("/user/Node"+ random)
	    targetChild ! msg
  		  
  		case _ => 
  		  println ("This algorithm is not supported")
  
	}
	
		val dur = Duration.create(50, scala.concurrent.duration.MILLISECONDS);
        val me = context.self
        context.system.scheduler.scheduleOnce(dur, me, "z")
	
	// Now Lets Handle the message to the parent from the Network Nodes. 
	def receive = {
    
	  case msg:String =>    
	    
	    msg.toLowerCase().head match {
	      
	      // Check if the Network node is sending the done message
	      case 'd' => 

			var received = msg.tail.split(",")
			if (received.length >0)
			networkNodeState.update((received(0).toInt -1), true)

			var donecount =0;
			for (x <- networkNodeState)
			  if(x == true) donecount += 1
	      
		    println("Time taken: " + (System.currentTimeMillis() - startTime).toString+"ms")
				
			if (received.size <= 1){
		        var percentCovered = (donecount*100.0/nodecount)
		        println("Network Coverage : "+percentCovered.toString)
		        if(percentCovered > 10.0 && percentCovered < 15.0) {		          
		          // Killing 10 % of the nodes.
		          deadnodescount = (nodecount * 10) / 100
		          self ! "z"
		        }
		        
		        if(percentCovered > 95.0) {
		          context.children.foreach(context.stop(_))
		          println ("Shutting Down the Network")
		          context.stop(self)
		          System.exit(1)
		        }
		    }else {
	        	println("Final Ratio: "+received(1))
	        	context.children.foreach(context.stop(_))
		        context.stop(self)
		        System.exit(1)
		     }
			
	      case 'z'=>
              for(i <-0 until deadnodescount) {
                var random = Int.MaxValue
                while (random > nodecount || random == 0) {
                  random = (Random.nextInt % nodecount).abs
                }
                val targetNode = childContext.actorSelection("/user/Node"+ random)
                targetNode ! "k" 
              }
            case 'x'=> 
                for(elem <- context.children) elem ! "a"+msg.tail
	      
	      
	    }
	    
	    // Lets handle the done message here. 
	  }	
}