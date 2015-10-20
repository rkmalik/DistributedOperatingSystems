package pastsim

import akka.actor._
import scala.math._
import scala.util.Random
import akka.actor.Actor._
import java.util.UUID
import java.lang.Long
import scala.collection.mutable._
import java.security.MessageDigest
import scala.collection.immutable.TreeMap

object project3 {
    val bits=32
    val base=4
    val Length=math.pow(2,base).toInt
    def main(args: Array[String]): Unit = {
    
    if(args.length < 2){
      println("Need two inputs!")
      return
    }
    
    val numNodes = args(0).toInt
    val numRequests = args(1).toInt
    var master=system.actorOf(Props(new Master(numNodes,numRequests)))  
  }
    
      /* to generate random ip address of the nodes excluding those already in the pastry network */
    def getIp(network: HashMap[String,ActorRef]): String = {
    val ip = scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256)
    if (!network.contains(ip)) 
      ip 
    else 
      getIp(network)
  } 
    
      /*Hashing being done*/
    def md5(num:Long): String ={
	 val leng = bits/base
	 val digit = Long.toString(num,math.pow(2,base).toInt)
	 var splchar = ""
	 for(i <- 0 until leng-digit.length){
	   splchar = splchar+"0"
	 }
	 splchar+digit
  }  
  
  val system =ActorSystem("project3")
  case class join(ID:String,path:List[String],NetWork:HashMap[String, ActorRef])
  case class sendState(PastrynodeID:String,path:List[String],lowerLeaf:Array[String],higherLeaf:Array[String],routeT:Array[Array[String]])
  case class Msg(index:Int,PastrynodeID:String,key:String,path:String,hopnum:Int,NetWork:HashMap[String, ActorRef])
  case class MsgArv(index:Int,PastrynodeID:String,key:String,path:String,hopnum:Int)
  case class complete(indexID:Int,totalhopcnt:Int)
  case class Init(NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class initial(NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class sendMessage(network:HashMap[String, ActorRef])
  case class lastjoin(lowerLeaf:Array[String],higherLeaf:Array[String],NetWork:HashMap[String, ActorRef])
  

class Node(nodeindex:Int, ID: String,numRequests:Int,master:ActorRef  ) extends Actor {

   val indexID=nodeindex
   val PastrynodeID=ID
   val boss=master
   val routeT=new Array[Array[String]](bits/base)
   val lowerLeaf=new Array[String](Length/2)
   val higherLeaf=new Array[String](Length/2)
   var Arvcount=0
   var totalhopcnt=0

   
   def findID(prefixR:String, Sortnode:Array[String],beginN:Int,endN:Int):String={
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
     val index=Sortnode.indexOf(PastrynodeID) 
 
     for(i<-0 until Length/2){
        lowerLeaf(i)={if(index>=Length/2-i)Sortnode(index-Length/2+i)else Sortnode(0)}
        higherLeaf(i)={if(index<Sortnode.size-i-1)Sortnode(index+i+1)else Sortnode.last}
     }
  
     // initialize the routetable
     for(i<-0 until routeT.size){
         var prefixR =PastrynodeID.substring(0, i)
         var digitR=PastrynodeID.substring(i, i+1)
         var IntdigitR=Integer.parseInt(digitR,16)
         for(j<-0 until routecol){
             if(j==IntdigitR){routeT(i)(j)=PastrynodeID}
             else if(j<IntdigitR){routeT(i)(j)=findID(prefixR+Integer.toString(j,16),Sortnode,0,index)}
             else{
               if (index+1<Sortnode.size){routeT(i)(j)=findID(prefixR+Integer.toString(j,16),Sortnode,index+1,Sortnode.size)}
             }          
         }        
     }
     self! sendMessage(network)
   } 
   def getroute(thekey:String):String = { 
       if (thekey==null ||lowerLeaf(0)==null||higherLeaf(0)==null||lowerLeaf.last==null||higherLeaf.last==null){return PastrynodeID}
       var cha=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(PastrynodeID, 16))
       var Minindex= -1
       if(Long.parseLong(thekey, 16)>=Long.parseLong(lowerLeaf(0), 16) && Long.parseLong(thekey, 16)<=Long.parseLong(PastrynodeID, 16)){ 
            for(i<- 0 until lowerLeaf.size){
               val tempvalue=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(lowerLeaf(i), 16))
               if (tempvalue<=cha){
                      cha=tempvalue
                      Minindex=i
                      
                 }
            }
         if (Minindex<0) return PastrynodeID else return lowerLeaf(Minindex)
       }
       else if(thekey > PastrynodeID && thekey <= higherLeaf(higherLeaf.size-1)){ 
           for (i<- 0 until higherLeaf.size){
                val tempvalue=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(higherLeaf(i), 16))
                if (tempvalue<=cha){
                      cha=tempvalue
                      Minindex=i                      
                 } 
           }
           if (Minindex<0) return PastrynodeID else return higherLeaf(Minindex) 
       }
       else{  // find in the route table
         var mcp=(thekey,PastrynodeID).zipped.takeWhile(Function.tupled(_==_)).map(_._1).mkString
         var lenOfmcp=mcp.length
         var digitR=Integer.parseInt(thekey.substring(lenOfmcp, lenOfmcp+1), 16) //first digit not in common
         if (routeT(lenOfmcp)(digitR)!=null){
             return routeT(lenOfmcp)(digitR)
         }
         else{
           val row=routeT(lenOfmcp) 
           for(i<-0 until row.size){
               val tempvalue=if (row(i)==null) cha else math.abs(Long.parseLong(thekey, 16)-Long.parseLong(row(i), 16))
               if (tempvalue<cha){
                  cha=tempvalue
                  Minindex=i
               }
           }
           if(Minindex>=0){return row(Minindex)}
           else if(thekey<PastrynodeID) return lowerLeaf(0)
           else return higherLeaf.last
         }         
       }     
   }
  def receive ={
    case Init(network,nodemap)=>initialize(network,nodemap) 
         
    case initial(network,nodemap)=>{            
                  if(indexID!=0){
                         var AID=nodemap(indexID-1)
                         network(AID)! join(PastrynodeID,List(PastrynodeID),network)                    
                                }
                     }
    case join(initalnode,path,network) => {
                 if (Long.parseLong(initalnode, 16)<Long.parseLong(PastrynodeID, 16)){
                       util.Sorting.quickSort(lowerLeaf)
                       if (Long.parseLong(initalnode, 16)>Long.parseLong(lowerLeaf(0), 16)){lowerLeaf(0)=initalnode}
                 }
                 else if (Long.parseLong(initalnode, 16)>Long.parseLong(PastrynodeID, 16)){
                       util.Sorting.quickSort(higherLeaf)
                       if (Long.parseLong(initalnode, 16)<Long.parseLong(higherLeaf.last, 16)){higherLeaf(Length/2-1)=initalnode}
                 }     

                 val routenode=getroute(initalnode)
                 if (routenode!=PastrynodeID){
                   network(routenode)! join(initalnode,path++List(PastrynodeID),network)
                   network(initalnode)! sendState(PastrynodeID,path++List(PastrynodeID),lowerLeaf,higherLeaf,routeT)
                   //based on mymap update self table
                  
                 }
                 else {
                   network(initalnode)! sendState(PastrynodeID,path++List(PastrynodeID),lowerLeaf,higherLeaf,routeT)
                   network(initalnode)! lastjoin(lowerLeaf,higherLeaf,network)
                   
                 } 
      
    }
    case sendState(pathnode,path,olleaf,orleaf,orouteT)=>{//update
      var lenOfpath=path.size
      routeT(lenOfpath-1)=orouteT(lenOfpath-1)  
       if (Long.parseLong(path.last, 16)<Long.parseLong(PastrynodeID, 16)){
                       util.Sorting.quickSort(lowerLeaf)
                       if (Long.parseLong(path.last, 16)>Long.parseLong(lowerLeaf(0), 16)){lowerLeaf(0)=path.last}
                 }
                 else if (Long.parseLong(path.last, 16)>Long.parseLong(PastrynodeID, 16)){
                       util.Sorting.quickSort(higherLeaf)
                       if (Long.parseLong(path.last, 16)<Long.parseLong(higherLeaf.last, 16)){higherLeaf(Length/2-1)=path.last}
                 } 
      
    }
    case lastjoin(olleaf,orleaf,network)=> { // updateleaf
       
          for(i<-0 until lowerLeaf.size){
                  if (Long.parseLong(olleaf(i), 16)<Long.parseLong(PastrynodeID, 16)){
                       util.Sorting.quickSort(lowerLeaf)
                       if (Long.parseLong(olleaf(i), 16)>Long.parseLong(lowerLeaf(0), 16)){lowerLeaf(0)=olleaf(i)}
                 }
                 else if (Long.parseLong(olleaf(i), 16)>Long.parseLong(PastrynodeID, 16)){
                       util.Sorting.quickSort(higherLeaf)
                       if (Long.parseLong(olleaf(i), 16)<Long.parseLong(higherLeaf.last, 16)){higherLeaf(Length/2-1)=olleaf(i)}
                 } 
                 if (Long.parseLong(orleaf(i), 16)<Long.parseLong(PastrynodeID, 16)){
                       util.Sorting.quickSort(lowerLeaf)
                       if (Long.parseLong(orleaf(i), 16)>Long.parseLong(lowerLeaf(0), 16)){lowerLeaf(0)=orleaf(i)}
                 }
                 else if (Long.parseLong(orleaf(i), 16)>Long.parseLong(PastrynodeID, 16)){
                       util.Sorting.quickSort(higherLeaf)
                       if (Long.parseLong(orleaf(i), 16)<Long.parseLong(higherLeaf.last, 16)){higherLeaf(Length/2-1)=orleaf(i)}
                 } 
          }
          self!sendMessage(network)
    }
    case sendMessage(network)=>{ 
               for(i<-1 to numRequests) {
               var key=md5((math.random*maximum).toLong)
               val routenode=getroute(key)
               if (routenode==PastrynodeID){
                 network(routenode)! MsgArv(i,PastrynodeID,key,"null",0)
               }
               else {network(routenode)! Msg(i,PastrynodeID,key,"",1,network)}
          } 
      
    }
      
    case Msg(j,initnode,key,path,hops,network)=>{
               val routenode=getroute(key)
               if (routenode==PastrynodeID){
                   network(initnode)! MsgArv(j,initnode,key,path+PastrynodeID,hops)
               }
               else {network(routenode)! Msg(j,initnode,key,path+PastrynodeID+"+",hops+1,network)}               

    }
     case MsgArv(j,initnode,key,path,hops)=>{
           Arvcount+=1
           totalhopcnt+=hops
           println("Pastry node with nodeID "+PastrynodeID+" receives the message number "+j+" and number of hops is "+hops)
           if(Arvcount==numRequests){
               master! complete(indexID,totalhopcnt)
           }
                     
    }
  // case "stop"=>context.system.shutdown()
    case "stop" =>System.exit(0)
    case _=>{}
  } 

}
 val maximum=math.pow(2,bits).toLong
class Master(numNodes: Int, numRequests:Int) extends Actor with ActorLogging {
    var count = 0
    var totalhopcount=0
    var NetWork = HashMap.empty[String, ActorRef] 
    var mymap = new TreeMap[Int,String]
 
    override def preStart() {
        for (i <- 0 until numNodes){
                var IP=getIp(NetWork)
                var ID=md5((math.random*maximum).toLong)
                  mymap +={i->ID}
                
                var ss=i
                var node=system.actorOf(Props(new Node(ss,ID, numRequests, self)))
               NetWork +={ID->node}
                                
        }
        for(allnodes<-NetWork.keysIterator){NetWork(allnodes)! Init(NetWork,mymap)}   
        
    }
   def receive = {
       case complete(nodeindex,nodehop) =>{
               totalhopcount+=nodehop
               count+=1
               println(count)
               if (count==numNodes){
                 println("Average hop for the pastry network is :"+(totalhopcount/(numRequests.toDouble*numNodes.toDouble)))
                 for(allnode<-NetWork.keysIterator){NetWork(allnode)!"stop"}
                //context.system.shutdown()
                 System.exit(0)
               }
               
         
       }
       case _ =>{}
  }
 }
  
}
