import scala.util.Random
import akka.actor.{ Actor, ActorRef, Props, ActorSystem }
import akka.actor.actorRef2Scala
import akka.dispatch.ExecutionContexts.global
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration.DurationInt
import scala.io.Source.fromFile
import java.security.MessageDigest
import akka.routing.RoundRobinRouter
import akka.util.Timeout
import scala.concurrent.duration._
import akka.pattern.ask
import akka.dispatch.ExecutionContexts._
import scala.collection.mutable.ArrayBuffer
import scala.AnyVal
import java.math.BigInteger
import java.lang.Integer
import scala.util.control.Breaks._
import scala.math._
import akka.pattern.pipe
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure} 
import scala.concurrent.Await
import scala.concurrent.Future
import scala.collection.immutable
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._
import scala.concurrent.duration._

case class SetValueOfKeyId(valueIdentifier : Int)
case object Initialize
case class NodeJoin(existingNode : Int)
case object GetSuccessor
case class SuccessorReceived(successorReceived : Int)
case class SetSuccessor(succ : Int)
case class SetPredescessor(pre : Int)
case class FindSomeId(w : Int)
case class LookUpTheKey(w : Int)
case class SuccessorFindSomeId(d : Int)
case class Update(n : Int, i : Int)
case object FindSuccessor
case class TrackHopsWhileLookUpOfEachFile(countReport: Integer,foundTheFile:Integer,fileFoundId:Int,foundOnNode:Int)
case class AssignIdentifier(totalNodes:Int,sortedArray1:ArrayBuffer[Int],mVal : Int,isAlive:Int,arrayOfFailedNodes:ArrayBuffer[Int])

object RandSet {
  val random = util.Random 

  def rand (count: Int, lower: Int, upper: Int, sofar: Set[Int] = Set.empty): Set[Int] =
    if (count == sofar.size) sofar else 
    rand (count, lower, upper, sofar + (random.nextInt (upper-lower) + lower)) 
}

object Start extends App{

	override def main(args:Array[String]){
		println("Welcome to chord protocol system!!")
		implicit val system = ActorSystem("ChordProtocolSystem")
		var numNodes : Int = 5
		var numRequests : Int = 2
		var failureNodesCount : Int = 1
		if(args.length==3){
			numNodes = args(0).toInt
			numRequests = args(1).toInt
			failureNodesCount = args(2).toInt

			println("You enetered numNodes =" + numNodes)
			println("You entered numRequests = " + numRequests)		
			println("the maximum number of nodes that can fail : " + failureNodesCount)
		}
		if(failureNodesCount>numNodes-2){
				println("Enter a lesser failure number")
				system.shutdown
		}else{
		val actor = system.actorOf(Props(new ChordProtocolMaster(numNodes,numRequests,failureNodesCount)))
		actor ! Initialize
		}
	}
}

class ChordProtocolMaster(numNodes1 : Int,numRequests1 : Int,failureNodesCount: Int) extends Actor {

	override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: ArithmeticException      => Resume
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Escalate
    }

	var startTime: Long = 0
	var nodeIdArray = new ArrayBuffer[Int]()
	var sortedArray = new ArrayBuffer[Int]()
	private var counter : Int = 0
	var mValue : Int = 10

	var numNodes:Int = numNodes1
	var numRequests:Int = numRequests1

	if(numNodes <= 10){
		mValue = 10
	}else if(numNodes > 10 && numNodes <= 50){
		mValue = 15
		//numRequests = 0
	}else if(numNodes > 50 && numNodes <= 1000){
		mValue = 20
	}else if(numNodes > 1000 && numNodes <= 10000){
		mValue = 30
	}else if(numNodes > 10000){
		mValue = 30
		numNodes = 10000
	}
	if(mValue > 10){
		numRequests = 0
	}
	var numberOfFiles:Int = 10
	private var hopCountForEachLookUp : Int = 0
	private var countForNumRequests : Int = 0
	private var totalNumberOfHops : Int = 0
	private var trackingCounter : Int = 0
	
	// Handle the failure cases
	var randomKillCount:Int = failureNodesCount;
    var arrayOfFailedNodesIndex = new Array[Int](randomKillCount)
    var arrayOfFailedNodes = new ArrayBuffer[Int]()

    var arrayOfAllNodes = (1 to (numNodes-1)).toArray
    //var arrayOfFailedNodes = new Array[Int](randomKillCount)
    //println ("Here it is!!"+RandSet.rand (randomKillCount+1, 0,totalCalculatedNodes).mkString (" "))
    //arrayOfFailedNodes = Array.fill[Int](randomKillCount+1){scala.util.Random.nextInt(totalCalculatedNodes)}  
    var s = (RandSet.rand (randomKillCount,0,numNodes))
    arrayOfFailedNodesIndex = s.toArray

	//Handling failure ends

	var participatingNodes = new ArrayBuffer[ActorRef]()

	for(i <- 0 until numNodes){
		var toHash = "participatingNodes:"+i
		var nodeIdentifier = consistentHashing(toHash)
		if(arrayOfFailedNodesIndex.contains(i)){
			arrayOfFailedNodes += nodeIdentifier
		}
		participatingNodes += context.actorOf(Props[WorkerNode], name = "nodeIdentifier:"+nodeIdentifier)
		makeNodeIdArray(nodeIdentifier)
	}

	println("FailedNodesArray")
	arrayOfFailedNodes.foreach(println)

	for(i <- 0 until numNodes){
		if(arrayOfFailedNodesIndex.contains(i)){
        println("Oops! the "+i+" node has failed ")
        participatingNodes(i) ! AssignIdentifier(numNodes,sortedArray,mValue,0,arrayOfFailedNodes)
        }else{
        //println("the value of nodes working fine"+i)
        participatingNodes(i) ! AssignIdentifier(numNodes,sortedArray,mValue,1,arrayOfFailedNodes)
        }
	}

	//Updating the sorted array because of failures
	sortedArray = sortedArray.diff(arrayOfFailedNodes)

	def makeNodeIdArray(nodeId : Int){
		nodeIdArray += nodeId
		sortedArray = nodeIdArray.sorted
	}

	def consistentHashing(nodeName : String) : Int = {
		
		def cryptoCurrencyFinder(s: String): Int = {
			val md = MessageDigest.getInstance("SHA-1")	
			md.update(s.getBytes())
      	 	var output = md.digest.map("%02x".format(_)).mkString
			var a = hexToBin(output)
			return a
		}

		def hexToBin(s : String) : Int = {
  			var bigInt = new BigInteger(s,16).toString(2);
  			var truncBigInt = bigInt.substring(0,mValue) //change
  			var decimalValue = Integer.parseInt(truncBigInt, 2)
  			return decimalValue
		}

		var nodeId = cryptoCurrencyFinder(nodeName)	
		return nodeId
	}

	def createKeyId(sortedArray : ArrayBuffer[Int]) = {

		var k : Int = 0

		for(i <- 0 until numberOfFiles){
			
			var filename = "filename:"+i
			var keyIdentifier = consistentHashing(filename)
			 breakable{
			  for(j <- 0 until sortedArray.length){
			  	if(keyIdentifier <= sortedArray(j)){

			  		context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+sortedArray(j)) ! SetValueOfKeyId(keyIdentifier)
			  		break
			  	}
			  	else if(j== (sortedArray.length-1)){
			  		context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+sortedArray(0)) ! SetValueOfKeyId(keyIdentifier)
			  		break
			  	}
			 	}
			}
		}
	}

	def createRing(sortedArray : ArrayBuffer[Int]){
	 	context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+sortedArray(0)) ! SetPredescessor(sortedArray(sortedArray.length-1))
	 	context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+sortedArray(0)) ! SetSuccessor(sortedArray(1))
	 	for(i <- 1 until sortedArray.length-1){
	 		context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+sortedArray(i)) ! SetPredescessor(sortedArray(i-1))
	 		context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+sortedArray(i)) ! SetSuccessor(sortedArray(i+1))
	 	}
		context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+sortedArray(sortedArray.length-1)) ! SetPredescessor(sortedArray(sortedArray.length-2))
	 	context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+sortedArray(sortedArray.length-1)) ! SetSuccessor(sortedArray(0))	 	

	}
	
	def receive = {
		case Initialize => {
			startTime = System.currentTimeMillis()
      		createKeyId(sortedArray) 
      		createRing(sortedArray) 
      		var counter : Int = 0
      		arrayOfAllNodes = arrayOfAllNodes.diff(arrayOfFailedNodesIndex)
      		if(numRequests > 0){
	      		for(i <- arrayOfAllNodes){	      			
	      			for(j<- 0 until numRequests){
			      		var randomFileNumber = Random.nextInt(numberOfFiles)
			      		var filename1 = "filename:"+randomFileNumber
						var keyIdentifier1 = consistentHashing(filename1)
						println("Going to lookup the file" + keyIdentifier1 + " starting at the node : " + i)
						implicit val timeout = Timeout(25 seconds) //???
    					val future = participatingNodes(i) ? LookUpTheKey(keyIdentifier1)
    					
	      			}
	      		}
	      	}else{
	      		var k = arrayOfAllNodes(Random.nextInt(arrayOfAllNodes.size))
	      		println("initial node : "+participatingNodes(k))
	      		var i = Random.nextInt(numberOfFiles)
	      		var filename1 = "filename:"+i
				var keyIdentifier1 = consistentHashing(filename1)
	      		participatingNodes(k) ! LookUpTheKey(keyIdentifier1)
	      	}
		}

	  case TrackHopsWhileLookUpOfEachFile(countReport,foundTheFile,fileFoundId,foundOnNode) => {
      		 
      		 trackingCounter = trackingCounter + 1
      		
      		if(foundTheFile == 1){
      			countForNumRequests = countForNumRequests+1
      			//println("The fileFoundId " + fileFoundId + "was found on the node " + foundOnNode +"With total hops =" + hopCountForEachLookUp)
      			println("The fileFoundId " + fileFoundId + " was found on the node " + foundOnNode)
      			hopCountForEachLookUp = 0
      			if(numRequests == 0){
      				println("The nodes failed are"+failureNodesCount)
      				println("All files found successfully,system shutdown initiated! Please find summary below:")
	      			var averageHops : Double = totalNumberOfHops.toDouble
	      			println("Average number of hops per request : " + averageHops) 
	      			println("Awesome! Total time taken for the all the search is : " + (System.currentTimeMillis() - startTime) + " milliseconds")  
      				context.system.shutdown()
      			}
      		}else{
      			hopCountForEachLookUp = hopCountForEachLookUp + countReport
      			totalNumberOfHops = totalNumberOfHops + 1
      			//println("Look up going on for "+ fileFoundId)
      		}
      		
      		if( (foundTheFile == 1 && countForNumRequests == numRequests * (numNodes - failureNodesCount )) || trackingCounter == 5000 ){
      			println("The nodes failed are"+failureNodesCount)
      			println("All files found successfully,system shutdown initiated! Please find summary below:")
      			println("The total lookups done successfully is : " + countForNumRequests)
      			println("The total number of hops for all of them : " + totalNumberOfHops)
      			
      			var averageHops : Double = totalNumberOfHops.toDouble/(countForNumRequests)
      			println("Average number of hops per request : " + averageHops) 
      			println("Awesome!!!!! Total time taken for the all the search is : " + (System.currentTimeMillis() - startTime) + " milliseconds")  
      			context.system.shutdown()
      		}
      }

	}
}

class WorkerNode extends Actor {

	import context._
  case class customException(smth:String) extends Exception(smth)

  //Define all kinds of failures 
  case class nodeCrashFailureException(smth:String) extends Exception(smth)
  case class nodeRecieveOmissionFailureException(smth:String) extends Exception(smth)
  case class nodeCSendOmissionFailureException(smth:String) extends Exception(smth)
  case class nodeTimeoutFailureException(smth:String) extends Exception(smth)
  case class nodeResponseFailureException(smth:String) extends Exception(smth)

	var name = self.path.name 
	var nodeIdString = name.split(":").last
    var nodeId = nodeIdString.toInt 
	var keyIdArray = new ArrayBuffer[Int]()
	private var predecessor :Int = 0
	private var successor : Int = 0
	var fingerTable = scala.collection.mutable.Map[Int, Int]()
	var lengthArray : Int = 0
	var lookupNodes = new ArrayBuffer[ActorRef]()
	var valueArray = new ArrayBuffer[Int]()
	var successorReceived : Int = 1
	var master: ActorRef = null
	var sortedNodeIdArray = new ArrayBuffer[Int]()
	var mValue : Int = 10

	var deadNodesList = new ArrayBuffer[Int]()
  	var isActive : Int = 1
	 
var hop : Int = 0
	
	def handleCustomException(foo:Exception) = {
      println("here" + foo)  
      context.stop(self)
  	}

  	def throwRandomException(nodeNumberSelected:Int){
	    if(nodeNumberSelected % 6 == 0){
	      throw new nodeCrashFailureException("Node " + nodeNumberSelected +" Failed")
	    }else if(nodeNumberSelected % 6 == 1){
	      throw new nodeRecieveOmissionFailureException("Node " + nodeNumberSelected +" Failed")
	    }else if(nodeNumberSelected % 6 == 2){
	      throw new nodeCSendOmissionFailureException("Node " + nodeNumberSelected +" Failed")
	    }else if(nodeNumberSelected % 6 == 3){
	      throw new nodeTimeoutFailureException("Node " + nodeNumberSelected +" Failed")
	    }else if(nodeNumberSelected % 6 == 4){
	      throw new nodeResponseFailureException("Node " + nodeNumberSelected +" Failed")
	    }else if(nodeNumberSelected % 6 == 5){
	      throw new customException("Node " + nodeNumberSelected +" Failed")
	    }
  	}

	def find_file(id : Int) : Int = {

		val system = akka.actor.ActorSystem("system")
		import system.dispatcher
		var find_successorValue : Int = 0	
		var preId = closest_preceding_finger(id)
	  	find_successorValue = successor
	  	hop+=1
		return find_successorValue
	}

	def findMax(valueArray : ArrayBuffer[Int]) : Int = {
		var maxi : Int = 0 
		if(valueArray.length == 1){
			maxi = valueArray(0)
		}else if(valueArray.isEmpty){
			maxi = nodeId
		}
		else{
			maxi =valueArray(0)
			for(i <- 1 until valueArray.length){
				if(valueArray(i) > maxi)
				maxi = valueArray(i)
			}
		}
		return maxi
	}

	def find_successor(id : Int) : Int = {
		val system = akka.actor.ActorSystem("system")
		import system.dispatcher
		var find_successorValue : Int = 0	
		var preId = closest_preceding_finger(id)
	  	find_successorValue = successor
		return find_successorValue
	}

	def closest_preceding_finger(id : Int) : Int = {

		var toBeReturnedValue : Int = 0
		val system = akka.actor.ActorSystem("system")
		import system.dispatcher
		if(!(id > nodeId && id <= successor) && nodeId < successor){
		 
			for(value <- fingerTable.values){
				if (value <= id){
		 			valueArray += value
		 		}
		 		else toBeReturnedValue = nodeId
			}
			var maximum : Int = findMax(valueArray)
			implicit val timeout = Timeout(5 seconds)
			val future = ask(context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+maximum), FindSomeId(id)).mapTo[Int]
	  		future onComplete {
	  			case Success(x) => toBeReturnedValue = x
	  			case Failure(t) => println("An error has occured: " + t.getMessage)
	  		}
	  		return toBeReturnedValue

	  	}
	  	else if(!(id > nodeId && id <= successor) && nodeId > successor)	{
	  		toBeReturnedValue = nodeId
	  		return toBeReturnedValue
	  	}
	  	else{

	  		toBeReturnedValue = nodeId
	  		return toBeReturnedValue
	  	}		
	}


	def init_finger_table(alreadyPresent : Int) = {
		
		val system = akka.actor.ActorSystem("system")
		import system.dispatcher
		var successorOfNodeId : Int = 0
		
		implicit val timeout = Timeout(5 seconds)
      	val future = context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+alreadyPresent) ? GetSuccessor
      	val result = Await.result(future, timeout.duration).asInstanceOf[Int]
      	successor = result
      	
	  	fingerTable += (computeKey(0) -> successor)
	  	
	  	predecessor = alreadyPresent
	  	
	  	for(i <- 0 until mValue-1){
	  		
	  		if(computeKey(i+1) >= nodeId && computeKey(i+1) < fingerTable.apply(computeKey(i))){
	  			fingerTable += (computeKey(i+1) -> fingerTable.apply(computeKey(i)))
	  			
	  		}
	  		else{
	  			implicit val timeout = Timeout(5 seconds)
	  			val future = ask(context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+alreadyPresent) , FindSomeId(computeKey(i+1))).mapTo[Int]
	  			future onComplete{
	  				case Success(x) => {
	  				successorOfNodeId = x
	  				
	  				}
	  				case Failure(t) => println("An error has occured: " + t.getMessage)
	  				
	  			}
	  			fingerTable += (computeKey(i+1) -> successorOfNodeId)
	  		}
	  		//println("nodeId = "+nodeId)
	  	}
	}

	def update_others()={
		for(i <- 0 to mValue-1){
			var p = closest_preceding_finger((nodeId-pow(2,i)).toInt)
			context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+p) ! Update(nodeId,i)	
		}
	} 

	def update_finger_table(s : Int, i : Int)={
		if(s >= nodeId && s < fingerTable.apply(computeKey(i))){
			fingerTable += (computeKey(i) -> s)
			var p = predecessor
			context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+p) ! Update(s,i)
		}
	}

	def compute(startId : Int, sortedNodeIdArray : ArrayBuffer[Int]): Int={
		var succId : Int = 0
		breakable{
			for(j <- 0 until sortedNodeIdArray.length){
				if(startId <= sortedNodeIdArray(j)){
					succId = sortedNodeIdArray(j)
					break
				}
				else if(j==sortedNodeIdArray.length-1){
					succId = sortedNodeIdArray(0)
				}	
			}
		}
		return succId	
	}

	def computeKey(s : Int ) : Int = {
		return ((nodeId + pow(2,s))%pow(2,mValue)).toInt
	}

	def createFingerTable() = {
		for(i <- 0 until mValue ){
				
				var j : Int = 0
				var startId = ((nodeId + pow(2,i))%pow(2,mValue)).toInt
				var successorId =  compute(startId, sortedNodeIdArray)
				fingerTable += (startId -> successorId)
			}
	}

	def generateNextNodeForMe(toBeFoundKey : Int) : Int={
		var nextNode : Int = 0
		var max : Int = 0
		if(!(toBeFoundKey > nodeId && toBeFoundKey <= successor)){
			//println("here")	 
			for(value <- fingerTable.values){
				if (value <= toBeFoundKey){
					
		 			valueArray += value
		 		}
			}
			if(valueArray.isEmpty){
				nextNode = successor
			}
			else{
				max = findMax(valueArray)
				if(nodeId == max){
					nextNode = nodeId
				}
				else {
					nextNode = max
				}
			}
		}else{
			nextNode = successor
		}

		//Adding the condition to handle failure , If the selected node is a dead node , return the next sucesssor of it
		if(deadNodesList.contains(nextNode)){
			nextNode = successor
		}

		return nextNode
	}

	def receive = {

		case SetValueOfKeyId(keyIdentifier)=>{
		 	keyIdArray += keyIdentifier
		}

		case SetSuccessor(succ) => {
			successor = succ
		}

		case SetPredescessor(pre) => {
			predecessor = pre
		}

		case NodeJoin(presentNode) => {
		 	
		 	init_finger_table(presentNode)
		 	update_others()		 	
		}

		case LookUpTheKey(toBeFoundId) => { 
			//println(" Look up for toBeFoundId" + toBeFoundId)
			//Handling the failure cases , if the state is not alive , it does not do any computation and simply kills itself by throwing an exception
		if(isActive == 1){

			if(keyIdArray.contains(toBeFoundId)){
				master ! TrackHopsWhileLookUpOfEachFile(1,1,toBeFoundId,nodeId)
			}else{
				createFingerTable()
				//println("nodeId is : "+nodeId)
				var nextNode : Int = generateNextNodeForMe(toBeFoundId)
				if(nextNode == nodeId){
					master ! TrackHopsWhileLookUpOfEachFile(1,1,toBeFoundId,nodeId)
				}else{
					master ! TrackHopsWhileLookUpOfEachFile(1,0,toBeFoundId,nodeId)
					context.actorSelection("akka://ChordProtocolSystem/user/$a/nodeIdentifier:"+nextNode) ! LookUpTheKey(toBeFoundId)
				}
			}
		}else{

        try{
          
            println("The node"+ nodeId + "is failed")
            //throw new customException("Node " + nodeNumberSelected +" Failed")
            throwRandomException(nodeId)
          
        }catch {
          //case foo: FooException => handleFooException(foo)
          //case bar: BarException => handleBarException(bar)
          case foo: customException => println(foo);handleCustomException(foo)
          case foo: nodeCrashFailureException => println(foo);handleCustomException(foo)
          case foo: nodeRecieveOmissionFailureException => println(foo);handleCustomException(foo)
          case foo: nodeCSendOmissionFailureException => println(foo);handleCustomException(foo)
          case foo: nodeTimeoutFailureException => println(foo);handleCustomException(foo)
          case foo: nodeResponseFailureException => println(foo);handleCustomException(foo)
          case _: Throwable => println("Got some other kind of exception in gossip");context.stop(self);
        }

      }

		}

		case AssignIdentifier(totalNodes,sortedArray1,mVal,isAlive,arrayOfFailedNodes) => {
	      master = sender
	      sortedNodeIdArray = sortedArray1.diff(arrayOfFailedNodes)
	      //println("Sorted Array List")
	      //sortedNodeIdArray.foreach(println)
	      mValue = mVal
	      isActive = isAlive
      	  deadNodesList = arrayOfFailedNodes
		}

		case Update(n , i)=>{
			update_finger_table(n,i)
		}
	
	}	
}

