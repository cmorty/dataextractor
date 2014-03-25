
/**
 * Extract Data
 */

package de.fau.dryrun.dataextractor

import java.io.File
import scala.io.Source
import org.slf4j.LoggerFactory
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.collection.mutable.Buffer
import collection.JavaConversions._
import java.util.Date

sealed abstract class Data {
	def stackList:List[String]
	var timeStamp:Option[Date] = None
}
class ExpResult(val k:String, val v:Long) extends Data {
	def stackList = List[String](k, v.toString)
}

class Result(val node:Int, val k:String, val v:Long) extends Data {
	def stackList = List[String](node.toString, k, v.toString)
}



//TODO
//class ExpSnapshot(val time:Long, val k:String, val v:Long) extends Data
//class Snapshot(val node:Int, val time:Long, val k:String, val v:Long) extends Data



abstract class DataExtractor {	
	class FileExtractor {
		def ok:Boolean = true
	}
	abstract case class Lines() extends FileExtractor { //Pass all lines
		def parse(lines: List[String]) : Vector[Data]
	} 
	abstract case class Linear() extends FileExtractor { //Pass lines linear
		def parse(line:String):Vector[Data]
	}
	abstract case class Parallel() extends FileExtractor { // Pass lines in parallel
		def parse(line:String):Vector[Data]
	}
	case object Dont extends FileExtractor //Dont parse file
		
	protected def getFileExtractor(file:File):FileExtractor = Dont
	
	

	
	/*
	 * This function allows to parse the file first and then decide whether this was a valid extractor
	 */
	def ok = true
	
	def extractFile(file:File):Vector[Data] = {
		val fe = getFileExtractor(file) 
		val rv = fe match {
			case ec:Lines => ec.parse(Source.fromFile(file).getLines.toList); 
			case ec:Linear => Source.fromFile(file).getLines.foldLeft(Vector[Data]())(_ ++ ec.parse(_))
			case ec:Parallel => Source.fromFile(file).getLines.toStream.par.aggregate(Vector[Data]())(_ ++ ec.parse(_), _ ++ _)
			//case ec:Parallel => Source.fromFile(file).getLines.foldLeft(Buffer[Data]())(_ ++ ec.parse(_)).toVector
			case Dont => Vector[Data]() 
		}
		if(fe.ok == true) {
			val timeStamp = Some(new Date(file.lastModified))
			//TODO IF there is a timestamp from the raw data we should use that!
			rv.foreach(_.timeStamp = timeStamp)
			rv
		} else {
			Vector[Data]()
		}
		
	}
	
	def extractDir(dir:File):Vector[Data] = {
		val files = DataExtractor.getDirList(dir)
		val rv = files.par.aggregate(Vector[Data]())( (list, file) => {
			list ++ {
				if(DataExtractor.isDir(file)) {
					extractDir(file)
				} else {
					extractFile(file)
				}
			}
		}, _ ++ _)
		
		if(ok) rv else Vector[Data]() 
	}
	def apply():DataExtractor =  throw new Exception("DataExtractor has no Factory")
}

object DataExtractor{
	import scala.language.implicitConversions
	
	private[this] val log = LoggerFactory.getLogger(this.getClass)
	val listMap: collection.concurrent.Map[java.io.File, Array[java.io.File]] = new java.util.concurrent.ConcurrentHashMap[java.io.File,Array[java.io.File]]
	def getDirList(dir:java.io.File) = {
		listMap.getOrElseUpdate(dir, dir.listFiles)
	}
	val dirMap: collection.concurrent.Map[java.io.File, Boolean] = new java.util.concurrent.ConcurrentHashMap[java.io.File,Boolean]
	def isDir(file:java.io.File) = {
		dirMap.getOrElseUpdate(file, file.isDirectory)
	}
	
	class HexString(val s: String) {
		def hex:Int = {
			if(s.startsWith("0x")) {
				Integer.parseInt(s.drop(2), 16)	
			} else {
				Integer.parseInt(s, 16)
			}
		}
	}
	
	implicit def str2hex(str: String): HexString = new HexString(str)
	
	class MoteID(val id: String) extends AnyVal
	
	/**
	 * Extract the id of the node 
	 */
	def idExtract(id:String):Option[Int] = {
		val sp = id.split(":")
		if(sp(0).equals("urn") || sp(1).equals("urn")) {
			return Some(sp.last.hex)
		} 
		
		log.trace("Not urn, but \""  + id + "\"" )
		None
	}
	
	def apply():DataExtractor =  throw new Exception("DataExtractor has no Factory")
}




class Experiment(dir: File) {
	val log = LoggerFactory.getLogger(this.getClass)
	//log.debug("Parsing " + dir)
	 
	log.trace("\nExperiment: " + dir)
	
	
	val extractors:List[Unit => DataExtractor] = List(
			Unit => {new DEuip1},
			Unit => {new DEuip1 with DEuipSim1},
			Unit => {new DEuip1_rcv},
			Unit => {new DEuip1_rcv with DEuipSim1 with DEuip1_rcvSim}, 
			Unit => {new DEsizes},
			Unit => {new DEboot},
			Unit => {new DEboot with DEbootSim}
	)
	
	val config = Source.fromFile(dir.toString +"/conf.txt").getLines.map(_.split("=", 2)).map(e => e(0) -> e(1)).toMap		
	
	log.trace("\nExperiment: " + dir + " -> " + config.map({x => x._1 + ": " + x._2}).mkString(", "))
	
	//Handle dir with every extractor
	val data = extractors.par.aggregate(Vector[Data]())(_ ++ _().extractDir(dir), _ ++ _)
	
	val (start, end) =  data.foldLeft(new Date(Long.MaxValue) -> new Date(0))((dt, x) => {
		var s = dt._1
		var e = dt._2
		x.timeStamp match{
			case Some(ts) => {
				if(s.after(ts)) s = ts
				if(e.before(ts)) e = ts
			} 
			case None => 
		}
		s -> e
	})
	
	//Prepare results
	val results:Vector[Result] =  data.view.filter(_.isInstanceOf[Result]).map(_.asInstanceOf[Result]).toVector
	val resultsNodeKeyValueMap = {
		val rv = collection.mutable.Map[Int, collection.mutable.Map[String, Long]]()
		for(r <- results) {
			val cll = rv.getOrElseUpdate(r.node, collection.mutable.Map[String, Long]())
			cll += (r.k -> r.v)
		}
		//Convert to immutable
		rv.map(x => {x._1 -> x._2.toMap}).toMap
	}
	val resultNodes = {
		results.map(_.node).toSet
	}
	
	//Prepare experiment Results
	val expResults:Vector[ExpResult] =  data.view.filter(_.isInstanceOf[ExpResult]).map(_.asInstanceOf[ExpResult]).toVector
	val expResultsKeyValueMap = expResults.map(x =>  {x.k -> x.v}).toMap
		
	log.trace("Results: "  + results.size + " ExpResults " + expResults.size)
	//def snapshots =  data.filter(_.isInstanceOf[Snapshot]).map(_.asInstanceOf[Snapshot])
}
