package pastsim

import scala.collection.mutable._
import akka.actor._
import scala.util.Random
import akka.actor.Actor._
import java.util.UUID
import java.lang.Long
import scala.collection.immutable.TreeMap
import java.security.MessageDigest
import scala.math._
import java.io.PrintStream
import java.io.FileOutputStream

object project3bonus {
  
   var test=false 
   val bits=32
   val base=4
   val Length=math.pow(2,base).toInt
   //System.setOut(new PrintStream(new java.io.FileOutputStream("FailureModelOutput.txt")))
  def main(args: Array[String]): Unit = {
    
    if(args.length < 2){
      println("Need two inputs!")
      return
    }
    if(args.length==3 && args(2)=="fail"){
       test = true
    }
    val numNodes = args(0).toInt
    val numRequests = args(1).toInt
     var master=system.actorOf(Props(new Master(numNodes,numRequests,test)))  
  }
   
   	/* to generate random ip address of the nodes excluding those already in the pastry network */
	def getIp(network: HashMap[String,ActorRef]): String = {
		val ip = scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256)
		if (!network.contains(ip)) ip else getIp(network)
	}
     /*Hashing being done*/
    def md5(num:Long): String ={
	 val leng = bits/base
	 val digit = Long.toString(num,math.pow(2,base).toInt)
	 var spl = ""
	 for(i <- 0 until leng-digit.length){
	   spl = spl+"0"
	 }
	 spl+digit
  }
  // Rohit: Create the actor system
    val system =ActorSystem("PastryProject3Bonus")
	case class Join(ID:String,path:List[String],NetWork:HashMap[String, ActorRef])
	case class PassState(pastryNodeId:String,path:List[String],leftLeaf:Array[String],rightleaf:Array[String],routeT:Array[Array[String]])
	case class Msg(index:Int,from:String,pastryNodeId:String,key:String,path:String,hopnum:Int,NetWork:HashMap[String, ActorRef],treemap:TreeMap[Int,String])
	case class MsgArv(index:Int,pastryNodeId:String,key:String,path:String,hopnum:Int)
	case class complete(indexID:Int,sumhops:Int)
	// Initializes the network
	case class Init(NetWork:HashMap[String, ActorRef],treemap:TreeMap[Int,String])
	case class initial(NetWork:HashMap[String, ActorRef],treemap:TreeMap[Int,String])
	case class sendMessage(network:HashMap[String, ActorRef],treemap:TreeMap[Int,String])
	
	// Class to test the failure.
	case class Failure(category:String,seq:Int,from:String,Init:String,key:String,path:String,hops:Int,network:HashMap[String, ActorRef],treemap:TreeMap[Int,String])
	case class Wakeup(from:String)
	case class Contact(seq:Int,from:String,init:String,key:String,path:String,hops:Int,network:HashMap[String, ActorRef],treemap:TreeMap[Int,String])
	case class fconn(NetWork:HashMap[String, ActorRef],treemap:TreeMap[Int,String],wakeup:Int)
	case class fconnp(NetWork:HashMap[String, ActorRef],treemap:TreeMap[Int,String])
	case class setMode(a:Int,base:List[(String,Int)])
	case class failconf(id1:String,id2:String,fflag:Int)
  	

class PastryNode(nodeindex:Int, ID: String,numRequests:Int,master:ActorRef  ) extends Actor {

   val indexID=nodeindex
   val pastryNodeId=ID
   val boss=master
   val routeT=new Array[Array[String]](bits/base)
   val leftLeaf=new Array[String](Length/2)
   val rightleaf=new Array[String](Length/2)
   var Arvcount=0
   var sumhops=0
   /////////////////
   var statemap= HashMap.empty[String,Int] //store the state of nodes
   val failconns = HashMap.empty[String,Int]
	  var diestate = 0 
	 val retransmit = 3

   	  def setMode(state:Int,conns:List[(String,Int)]) = {
	    
	    diestate = state
	    if(conns!=null && conns.size>0){
	      conns.foreach( pair => failconns+= {pair._1 -> pair._2})
	    }
	  }
	  
   
   def searchNodeID(prefixR:String, Sortnode:Array[String],beginN:Int,endN:Int):String={
     for(m<-beginN until endN){
       if(Sortnode(m).startsWith(prefixR)) {return Sortnode(m)}       
     }
     return null
   }
   def initialize(network:HashMap[String, ActorRef],nodemap:TreeMap[Int,String])={
     val routecol=math.pow(2,base).toInt
     for(i<-0 until routeT.size){
       routeT(i)=new Array[String](routecol)
     }
     val Sortnode=nodemap.values.toArray
     util.Sorting.quickSort(Sortnode)
     val index=Sortnode.indexOf(pastryNodeId) 
     for(i<-0 until Length/2){
       
        leftLeaf(i)={if(index>=Length/2-i)Sortnode(index-Length/2+i)else Sortnode(0)}
        rightleaf(i)={if(index<Sortnode.size-i-1)Sortnode(index+i+1)else Sortnode.last}
      }
  
     // initialize the routetable
     for(i<-0 until routeT.size){
         var prefixR =pastryNodeId.substring(0, i)
         var digitR=pastryNodeId.substring(i, i+1)
         var IntdigitR=Integer.parseInt(digitR,16)
         for(j<-0 until routecol){
             if(j==IntdigitR){routeT(i)(j)=pastryNodeId}
             else if(j<IntdigitR){routeT(i)(j)=searchNodeID(prefixR+Integer.toString(j,16),Sortnode,0,index)}
             else{
               if (index+1<Sortnode.size){routeT(i)(j)=searchNodeID(prefixR+Integer.toString(j,16),Sortnode,index+1,Sortnode.size)}
             }          
         }
         
     }
      //set status map for all elements in lset and rtable
	    leftLeaf.foreach(x => if(x!=pastryNodeId) statemap+={x->0})
	    rightleaf.foreach(x => if(x!=pastryNodeId) statemap+={x->0})
	   
	    routeT.foreach( x => x.foreach(y => if(y!=pastryNodeId&&y!=null) statemap+={y->0}) )
	    
     //println("route table is:"+routeT)
     self! sendMessage(network,nodemap)
   } //end initialize
   
def alter(k:String, orig:String, curr:String,nodemap:TreeMap[Int,String]):String = {
	    
	    //val lset = slset++llset
	    val sortN = nodemap.values.toArray
	    util.Sorting.quickSort(sortN)
	    val index = sortN.indexOf(orig)
	    
	    var result = orig
	    var resindex = index
	    if(index==0 || (k>orig && index<sortN.length-1)) {result=sortN(index+1);resindex=index+1}
		    else { result=sortN(index-1);resindex=index-1 }
		    
	    while(result==curr){
		  resindex += resindex-index
		  if(resindex==sortN.size){resindex=index-1}
		  else if(resindex==0){ resindex = index+1}
		  result = sortN(resindex)
	    }
	    
	    return result
	    
	  }
	  
   
   def getroute(thekey:String):String = { 
           if (thekey==null ||leftLeaf(0)==null||rightleaf(0)==null||leftLeaf.last==null||rightleaf.last==null){return pastryNodeId}

       var cha=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(pastryNodeId, 16))
       var Mindex= -1
       
       if(thekey>=leftLeaf(0) && thekey<=pastryNodeId){ //if in the left leafs
            for(i<- 0 until leftLeaf.size){
               val tempcha=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(leftLeaf(i), 16))
               if (tempcha<=cha){
                      cha=tempcha
                      Mindex=i
                      
                 }
            }
         if (Mindex<0) return pastryNodeId else return leftLeaf(Mindex)
       }//end find in the left leaf
       else if(thekey > pastryNodeId && thekey <= rightleaf(rightleaf.size-1)){ //find in the right leafs
           for (i<- 0 until rightleaf.size){
                val tempcha=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(rightleaf(i), 16))
                if (tempcha<=cha){
                      cha=tempcha
                      Mindex=i                      
                 } 
           }
           if (Mindex<0) return pastryNodeId else return rightleaf(Mindex) 
       }
       else{  
         var matchparam=(thekey,pastryNodeId).zipped.takeWhile(Function.tupled(_==_)).map(_._1).mkString
         var lengthmatchParam=matchparam.length
         var digitR=Integer.parseInt(thekey.substring(lengthmatchParam, lengthmatchParam+1), 16) //first digit not in common
         if (routeT(lengthmatchParam)(digitR)!=null){
             return routeT(lengthmatchParam)(digitR)
         }
         else{
           val row=routeT(lengthmatchParam) //search in the same row
           for(i<-0 until row.size){
               val tempcha=if (row(i)==null) cha else math.abs(Long.parseLong(thekey, 16)-Long.parseLong(row(i), 16))
               if (tempcha<cha){
                  cha=tempcha
                  Mindex=i
               }
           }
           if(Mindex>=0){return row(Mindex)}
           else if(thekey<pastryNodeId) return leftLeaf(0)
           else return rightleaf.last
         }         
       }     
   }
  def receive ={
    case Init(network,nodemap)=>initialize(network,nodemap) //then find A whose address is next to self
  
    case initial(network,nodemap)=>{            
                  if(indexID!=0){
                         var AID=nodemap(indexID-1)
                         network(AID)! Join(pastryNodeId,List(pastryNodeId),network)                    
                                }
                     }
    case Join(initalnode,path,network) => {
                 if (Long.parseLong(initalnode, 16)<Long.parseLong(pastryNodeId, 16)){
                       util.Sorting.quickSort(leftLeaf)
                       if (Long.parseLong(initalnode, 16)>Long.parseLong(leftLeaf(0), 16)){leftLeaf(0)=initalnode}
                 }
                 else if (Long.parseLong(initalnode, 16)>Long.parseLong(pastryNodeId, 16)){
                       util.Sorting.quickSort(rightleaf)
                       if (Long.parseLong(initalnode, 16)<Long.parseLong(rightleaf.last, 16)){rightleaf(Length/2-1)=initalnode}
                 }     

                 val routenode=getroute(initalnode)
                 if (routenode!=pastryNodeId){
                   network(routenode)! Join(initalnode,path++List(pastryNodeId),network)
                   network(initalnode)! PassState(pastryNodeId,path++List(pastryNodeId),leftLeaf,rightleaf,routeT)
                   //based on treemap update self table
                  
                 }
                 else {
                   network(initalnode)! PassState(pastryNodeId,path++List(pastryNodeId),leftLeaf,rightleaf,routeT)
                   network(initalnode)! "lastjoin"
                   
                 } 
      
    }
    case PassState(pathnode,path,olleaf,orleaf,orouteT)=>{
      var lenOfpath=path.size
      routeT(lenOfpath-1)=orouteT(lenOfpath-1)  
      if (Long.parseLong(path.last, 16)<Long.parseLong(pastryNodeId, 16)){
                       util.Sorting.quickSort(leftLeaf)
                       if (Long.parseLong(path.last, 16)>Long.parseLong(leftLeaf(0), 16)){leftLeaf(0)=path.last}
                 }
                 else if (Long.parseLong(path.last, 16)>Long.parseLong(pastryNodeId, 16)){
                       util.Sorting.quickSort(rightleaf)
                       if (Long.parseLong(path.last, 16)<Long.parseLong(rightleaf.last, 16)){rightleaf(Length/2-1)=path.last}
                 } 
      
    }
   /* case "lastjoin"=> { // start to send message
          for(i<-1 to numRequests) {
               val routenode=getroute(pastryNodeId)
               if (routenode==pastryNodeId){
                 network(routenode)! MsgArv(i,pastryNodeId,"null",0)
               }
               else {network(routenode)! Msg(i,pastryNodeId,"",1)}
          }  
    }*/
    case fconn(fnetwork,ftreemap,wakeup)=>{
   //    println("receive fconnp")
      val conleaf = leftLeaf++rightleaf
      var id1=pastryNodeId
      val id2 = conleaf((math.random*conleaf.size).toInt)
      val conns1 = List[(String,Int)]((id2,wakeup)); setMode(1, conns1)
      val conns2 = List[(String,Int)]((id1,wakeup)); fnetwork(id2)!setMode(1, conns2)
      master! failconf(id1,id2,1)
      println(" Connection is terminated temporarily for PastryNode (" + id1 + ")<->N(" + id2 + ")")
    }
    case setMode(a,base)=>setMode(a,base)
    
    case fconnp(fnetwork,ftreemap)=>{
     // println("receive fconnp")
      val conleaf = leftLeaf++rightleaf
      var idp1=pastryNodeId
      val idp2 = conleaf((math.random*conleaf.size).toInt)
      val connsp1 = List[(String,Int)]((idp2,-1)); setMode(1, connsp1)
      val connsp2 = List[(String,Int)]((idp1,-1)); fnetwork(idp2)!setMode(1, connsp2)
      master! failconf(idp1,idp2,2)
      println("  (" + idp1 + ") <-> (" + idp2 + ")  connection is terminated permanently")
    }
    case sendMessage(network,nodemap)=>{ 
               for(i<-1 to numRequests) {
               var key=md5((math.random*maximum).toLong)
               val routenode=getroute(key)
               if (routenode==pastryNodeId){
                 network(routenode)! MsgArv(i,pastryNodeId,key,"null",0)
               }
               else {network(routenode)! Msg(i,pastryNodeId,pastryNodeId,key,"",1,network,nodemap)}
          } 
      
    }
      
    case Msg(j,from,initnode,key,path,hops,network,nodemap)=>{
  
               
	          if(diestate == 2 && (!statemap.contains(from) || statemap(from)>=0)){
	            network(from) ! Failure("Messg",j,pastryNodeId,initnode,key,path,hops,network,nodemap)
	          }
	          else if(diestate>0 && failconns.contains(from) && failconns(from)!=0){
	            network(from) ! Failure("Messg",j,pastryNodeId,initnode,key,path,hops,network,nodemap)
	            failconns +={from->(failconns(from)-1)}	            
	          }
	          else{	              
	              if(diestate>0 && failconns.contains(from) && failconns(from)==0){
		              failconns.remove(from)
		              if(diestate==1 && (failconns.size==0||failconns(from)==0)) { diestate=0 }
		              network(from) ! Wakeup(pastryNodeId)
	              }
	            
	           val routenode=getroute(key)
               if (routenode==pastryNodeId){
                   network(initnode)! MsgArv(j,initnode,key,path+pastryNodeId,hops)
               }
               else {network(routenode)! Msg(j,pastryNodeId,initnode,key,path+pastryNodeId+"+",hops+1,network,nodemap)} 

	          }

    }
    case MsgArv(j,initnode,key,path,hops)=>{
           Arvcount+=1
           sumhops+=hops
           //println("PastryNode "+pastryNodeId+" receives the "+j+"th message, the path is "+path+" hops is "+hops)
           println("PastryNode "+pastryNodeId+" receives the message Count "+j+" \t and the count of Path "+path+" Hops is: \t "+hops)
           
           if(Arvcount==numRequests){//the PastryNode is finished
               master! complete(indexID,sumhops)
           }
                     
    }
    case "stop"=>context.system.shutdown()
    case Failure(category,seq,from,initnode,key,path,hops,network,nodemap) => {
	          
	          //check whether the first time to meet the failure
	          if(statemap(from)<retransmit){
	              
	              println( "The nodeid (" + pastryNodeId + ") temporarily terminates which is Connected to nodeid (" + from + ") " )
	            
		          statemap += {from->(statemap(from)+1)}

		          network(from) ! Msg(seq,pastryNodeId,initnode,key,path+pastryNodeId+"+",hops+1,network,nodemap)
	          }
	          else{
	            var next = alter(key,from,pastryNodeId,nodemap)
	            
	            println( "The nodeid (" + pastryNodeId + ") permanently terminates which is Connected to nodeid (" + from + ") !" +"\n"
	                + "so now utilise (" +next + ") and replace the nodeid: " + from  )
	            
	            network(from) ! Contact(seq,pastryNodeId,initnode,key,path,hops,network,nodemap)
	            
	          }
	}
	        
  case Wakeup(from) => {
	          println("The node N" + indexID + "(" + pastryNodeId + ")  connect with node N"  
	                + "(" + from + ") wake up!")
	        }
 case Contact(seq,from,initnode,key,path,hops,network,nodemap) => {
	          if(diestate>0 && failconns.contains(from) && failconns(from)<0){
	            failconns.remove(from) 
	            if(diestate==1 && failconns.size==0) { diestate=0 }
	          }
	          else if(diestate==2){
	              
	            //println("add N"+map(from).intId + " to " + statemap(from))
	            statemap+=(from -> (-1))
	              
	          }
	          val routenode=getroute(key)
               if (routenode==pastryNodeId){
                   network(initnode)! MsgArv(seq,initnode,key,path+pastryNodeId,hops)
               }
               else {network(routenode)! Msg(seq,pastryNodeId,initnode,key,path+pastryNodeId+"+",hops+1,network,nodemap)} 
	          	          	          
	        }
    case _=>{}
  } 

}
 val maximum=math.pow(2,bits).toLong
class Master(numNodes: Int, numRequests:Int,test:Boolean) extends Actor with ActorLogging {
    var count = 0
    var totalhops=0
     var NetWork = HashMap.empty[String, ActorRef] 
    var treemap = new TreeMap[Int,String]
   
  

    override def preStart() {
     // println(test)
        for (i <- 0 until numNodes){
                var IP=getIp(NetWork)
                var ID=md5((math.random*maximum).toLong)
                  treemap +={i->ID}
                
                var ss=i
                var PastryNode=system.actorOf(Props(new PastryNode(ss,ID, numRequests, self)))
               NetWork +={ID->PastryNode}
                                
        }
        //println(treemap)
     for(allnodes<-NetWork.keysIterator){NetWork(allnodes)! Init(NetWork,treemap)}   
     if(test){      
      //generate randomly the connection who dies temperarily
    	 		 val wakeup = 2
    			 val n1 = (math.random*treemap.size+1).toInt
    			 NetWork(treemap(n1)) ! fconn(NetWork,treemap,wakeup)
    			 //val n1leaf = NetWork(treemap(n1)).slset++map(intmap(n1)).llset  
                 var np1 = (math.random*treemap.size+1).toInt
                 while(np1==n1){ np1 = (math.random*treemap.size+1).toInt }
                 NetWork(treemap(np1)) ! fconnp(NetWork,treemap)
                 println("========================The failure node propagation start below=================")
         }   
    }
    var failcount=0
    var id1="";var id2="";var id3="";var id4=""
   def receive = {
       case failconf(n1,n2,flag)=>{
         if (flag==1){var id1=n1; var id2=n2;failcount =failcount+1}
         if (flag==2){var id3=n1; var id42=n2;failcount =failcount+1}
         if (failcount==2){
             var failnd = (math.random*treemap.size+1).toInt
             var failndID = treemap(failnd)
             while(List(id1,id2,id3,id4).contains(failndID)){ failnd = (math.random*treemap.size+1).toInt; failndID= treemap(failnd)}
             NetWork(failndID)!setMode(2,null)
             println("The PastryNode N:"+ failnd + "(" + failndID + ")" + "is terminated \n")
         }
         
       }
       case complete(nodeindex,nodehop) =>{
               totalhops+=nodehop
               count+=1

               if (count==numNodes){
                 println("So,the average Hops for the Network :"+(totalhops/(numRequests.toDouble*numNodes.toDouble)))
                 for(allnode<-NetWork.keysIterator){NetWork(allnode)!"stop"}
                 context.system.shutdown()
               }
               
         
       }
       case _ =>{}
  }
 }
  
}
