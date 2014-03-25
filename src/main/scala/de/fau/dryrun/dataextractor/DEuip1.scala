package de.fau.dryrun.dataextractor


import java.io.File
import DataExtractor._
import org.slf4j.LoggerFactory
import scala.util.Try
import scala.util.Failure
import scala.util.Success

class DEuip1 extends DataExtractor {
	val log = LoggerFactory.getLogger(this.getClass)

	val rst = List("tx", "rx", "reliabletx", "reliablerx", "rexmit", "acktx", "noacktx",
		"ackrx", "timedout", "badackrx", "toolong", "tooshort", "badsynch", "badcrc",
		"contentiondrop", "sendingdrop", "lltx", "llrx").map("rime_" + _)
	val ust = List("recv", "sent", "forwarded", "drop", "vhlerr", "hblenerr",
			"lblenerr", "fragerr", "chkerr", "protoerr").map("ip_" + _ ) :::
			List("recv", "sent", "drop", "typeerr", "chkerr").map("icmp_" + _ ) :::
			List("recv", "sent", "drop", "chkerr", "ackerr","rst", "rexmit", "syndrop", 
					"synrst").map("tcp_" + _) :::
			List("drop", "recv", "sent", "chkerr").map("udp_" + _) :::
			List("drop", "recv", "sent").map("nd6_" + _)
			
	val egt = List("CPU", "LPM", "TX", "RX").map("ener_" + _)
	
	
	def extract(s:String) = idExtract(s)
	val filename = "wisebed.log"
	
	private def zipToRes(id:Int, k:List[String], dat:String):(Boolean, Vector[Data])  = {
		val v = dat.split(" ")
		if(k.size != v.size) {
			log.warn("Does not match:\n" + k.mkString(", ") + "\n" + v.mkString(", "))
			return(false -> Vector[Result]())
		}
		val vInt = Try(v.map(_.toInt))
		
		if(vInt.isFailure) {
			log.warn("Could not convert to Int: " + v.mkString(", "))
			return(false -> Vector[Result]())
		}
		
		
		true -> k.toVector.zip(vInt.get).map(x => {new Result(id, x._1, x._2)})
	}
	
	override def getFileExtractor(file:File):FileExtractor = {

		
		//log.debug("Checking file " + file + " - Name: " + file.getName)
				
		if(file.getName.equals(filename)){
			//log.debug("New Parallel")
			new Parallel() {

				override def parse(line:String):Vector[Data]  = {
					//log.debug("Parsing " + line)
					val s = line.split(" ", 3)
					if(s.size == 2) {
						Vector[Data]()	
					} else extract(s(0)) match {
						case None => Vector[Data]()
						case Some(id) => {
							//log.trace("data:  -" + line + "-")
							
							s(1) match {
								case "RS:" => zipToRes(id, rst, s(2))._2
								case "US:" => zipToRes(id, ust, s(2))._2
								case "EG" => zipToRes(id, egt, s(2))._2
								case "EG:" => zipToRes(id, egt, s(2))._2
								case _ => Vector[Data]()
							}
							
						}
					}
					
				}
			}
		}
		else 
			Dont
	} 
	
	override def apply() = new DEuip1
}

trait DEuipSim1 extends DEuip1 {

	override val filename = "motes.log"
		
		
	override def extract(s:String) = {
		Try(s.dropRight(1).toInt) match {
			case Failure(_) => None
			case Success(id) => Some(id)
		}
	}	
	
}

