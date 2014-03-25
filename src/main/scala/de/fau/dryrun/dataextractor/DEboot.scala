package de.fau.dryrun.dataextractor

import scala.collection.immutable.Vector
import java.io.File
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import DataExtractor._
import org.slf4j.LoggerFactory


class DEboot extends DataExtractor {
	val log = LoggerFactory.getLogger(this.getClass)
	val filename = "wisebed.log" 
	
	//Make extract part of class
	def extract(s:String) = idExtract(s)
		
	override def getFileExtractor(file:File):FileExtractor = {
		if(file.getName.equals(filename)){
			//log.debug("New Parallel")
			new Lines() {
				
				override def parse(lines: List[String]) : Vector[Data] = {
					val bootMap = collection.mutable.Map[Int, Int]()
					val bootlines = lines.view.filter(_.contains(" Starting "))
					for(line <- bootlines) {
						extract(line.split(" ")(0)) match {
							case Some(id) => bootMap += id -> (bootMap.getOrElse(id,0)  + 1)
							case None => log.error("Could not get id for: " + line)
						}
					}
					bootMap.map(x => new Result(x._1,"meta_boot", x._2)).toVector
				}
			}
		} else {
			Dont
		}
	}

}


trait DEbootSim extends DEboot {

	override val filename = "motes.log"
		
		
	override def extract(s:String) = {
		Try(s.dropRight(1).toInt) match {
			case Failure(_) => None
			case Success(id) => Some(id)
		}
	}	
	
}
