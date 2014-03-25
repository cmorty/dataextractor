package de.fau.dryrun.dataextractor

import java.io.File
import org.slf4j.LoggerFactory
import scala.util.Try
import scala.io.Source

class DEuip1_rcv extends DEuip1 {
    
	def getBackup(id:Int):Option[Int] = DEuip1_rcv.backupMap.get(id)
	var missKeyReported = false 
	
	override def getFileExtractor(file:File):FileExtractor = {		
		//log.debug("Checking file " + file + " - Name: " + file.getName)
				
		if(file.getName.equals(filename)){
			//log.debug("New Parallel")
			new Lines() {
				val dat = collection.mutable.Map[String, Int]()
				override def parse(lines: List[String]) : Vector[Data] = {
					import DataExtractor._
					val splitlines = lines.map(_.split(" "))
					//urn:fau:0x000a: MAC 00:12:74:00:0e:d5:30:12 Contiki 2.6 started. Node id is set to 10.
					val nids = splitlines.view.map(x => {extract(x(0))}).filter(_.isDefined).map(_.get).toSet
					
					val idlines = splitlines.filter(x => {(x.size == 12 | x.size == 11)  && x(1).equals("MAC")})
					log.trace("IDlines: \n" + idlines.map(_.mkString(" ")).mkString("\n"))
					
					//idMap maps the output id to the real node id
					val idMap = collection.mutable.Map[Int, Int]()
					
					for(l <- idlines) {
						
						val mid = extract(l(0)).get
						val mac = l(2).split(":").map(_.hex) 
						val id = mac(7)
						val id2 = 256 * 256 * mac(5) + 256 * mac(6) + mac(7) // This breaks with ... 00:00:XX 
						idMap += id -> mid
						idMap += id2 -> mid
						
					}
					
					{
						val unass = nids.filterNot(idMap.values.toSet.contains(_))
						if(unass.size > 0)log.debug("Unassigned nodes: " +  unass.mkString(", ") )
					}
					
					DEuip1_rcv.backupMap ++= idMap
					
					log.trace("IDmap: " + idMap.map(x => {x._1 + " -> " + x._2}).mkString(", ") )
					
					val parselines = splitlines.filter(x => {x.size == 10 && x(2).equals("recv")})
					
					val rcvMap = collection.mutable.Map[Int, Int]()
					for(l <- parselines)  {
						val src = {if(l(9).contains(".")){
								val els = l(9).split('.').map(_.hex)
								256 * 256 * els(0) + 256 * els(1) + els(2)
							} else {
								l(9).toInt
							}
						}  
								
						
						val ctr = rcvMap.getOrElse(src, 0)
						rcvMap += src -> (ctr + 1) 
					}
					for(key <- rcvMap.keys) if(!idMap.contains(key)) {
						val b = getBackup(key)
						if(b.isDefined) {
							if(!missKeyReported) {
								missKeyReported = true
								log.debug("Missing key  \""  + key + "\" in " + filename +". No more missing keys will be reported for this file.")
							}
							idMap += (key -> b.get)
						} else {
							log.error("Missing key  \""  + key + "\" in " + filename + " Could not Recover")
							return Vector[Data]()
						}
					}
					DEuip1_rcv.backupMap.map(x => new Result(x._2,"meta_packrcv", rcvMap.getOrElse(x._1, 0).toLong)).toVector
					rcvMap.map(x => new Result(idMap(x._1),"meta_packrcv", x._2)).toVector
				}
			}
				
		}
		else 
			Dont
	} 
}

object DEuip1_rcv{
	import collection.JavaConversions._
	val backupMap:collection.concurrent.Map[Int, Int] = new java.util.concurrent.ConcurrentHashMap[Int, Int]()
	def savemap(file:File) {
		val outfile = new java.io.PrintWriter(file)
		outfile.print(backupMap.map(x => ("" + x._1 + "\t" + x._2)).mkString("\n")) 
		outfile.close
	}
	
	def loadmap(file:File) {
		Try{
			backupMap ++= Source.fromFile(file).getLines.map(_.split("\t").map(_.toInt)).map(x => (x(0) -> x(1)))
		}
	}
	
}

trait DEuip1_rcvSim extends DEuip1_rcv {
	override def getBackup(id:Int):Option[Int] = Some(id)
}
