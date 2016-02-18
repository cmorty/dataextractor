
package de.fau.dryrun.dataextractor

import scala.io.Source
import java.io.File
import scala.collection.mutable.Buffer
import org.slf4j.LoggerFactory
import org.apache.log4j.Logger
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.PatternLayout
import scopt.mutable.OptionParser
import scopt.mutable.OptionParser._
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.log4j.Level






object DataCollector {
	val data = collection.mutable.Map[String, Experiment]()
	val log = LoggerFactory.getLogger(this.getClass)

	val sep = ", "
	val nodeStr = "node"
		
		
	private def getStartEnd(experiments:Array[Experiment]) = {
		var s = new Date(Long.MaxValue)
		var e = new Date(0)
		for(exp <- experiments) {
			if(s.after(exp.end)) s = exp.end
			if(e.before(exp.end)) e = exp.end
			
		}
		s -> e
	}
	
	def main(args: Array[String]): Unit = {
		val DEFAULT_PATTERN_LAYOUT = "%-23d{yyyy-MM-dd HH:mm:ss,SSS} | %-30.30t | %-30.30c{1} | %-5p | %m%n"
		Logger.getRootLogger.addAppender(new ConsoleAppender(new PatternLayout(DEFAULT_PATTERN_LAYOUT)))
		
		val dp=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");	
		var infolder = "" 
		var outfile = ""
		var startDate:Date = null
		var endDate:Date = null
		var dryRun = false
		var ignoreFile:File = null
		val parser = new OptionParser("scopt") {
		  arg("<infolder>", "<infolder> input file", { v: String => infolder = v })
		  argOpt("[<outfile>]", "<outfile> output file", { v: String => outfile = v })
		  opt("s","start", "When to start collecting data in yyyy-MM-ddTHH:mm:ss",  {v: String => startDate = dp.parse(v)})
		  opt("e", "end", "When to stop collecting data in yyyy-MM-ddTHH:mm:ss",  {v: String => endDate = dp.parse(v)})
		  opt("n", "dryrun", "Dont write to file", {dryRun = true} )
		  opt("l","loglevel", "Loglevel (off, fatel, error, warn, info, debug ,trace)", {
		  	v:String => {
		  		val lv = Level.toLevel(v)
		  		log.info("Setting loglevel to " + lv.toString())
		  		Logger.getRootLogger.setLevel(lv)
		  	}
		  })
		  opt("i", "ignore", "File of experiments to ignore - one per Line", {v:String => ignoreFile = new File(v) })
		  help("h", "help", "prints this usage text")
		  
		  // arglist("<file>...", "arglist allows variable number of arguments",
		  //   { v: String => config.files = (v :: config.files).reverse })
		}
		
		if(!parser.parse(args)) sys.exit(1)
		
		
		
		val folder = new File(infolder)
		val backupfile = new File(folder + "/" + "idmap.back")
		log.info("Reading: " + folder)
		DEexp_udp.loadmap(backupfile)
		

		val ignorelist = {if(ignoreFile == null) List[String]() else Source.fromFile(ignoreFile).getLines.toList}
		
		log.trace("BV: " +  folder.listFiles.size) 
		log.trace("AV: " + folder.listFiles.filter({x => !(ignorelist.contains(x.getName))}).size)
		
		
		// Using par here does not improve performance		
		//val experiments = folder.listFiles.filter(_.isDirectory).par.map(new Experiment(_))
		val experiments = folder.listFiles.view.filter({x => !(ignorelist.contains(x.getName))}).filter(_.isDirectory).map(new Experiment(_)).toArray		
		
		DEexp_udp.savemap(backupfile)
		
		log.info("Tatal Experiments: " + experiments.size)
		log.info("Total Data: " + experiments.map(_.data.size).sum)
		
		val (tsDate, teDate ) = getStartEnd(experiments)
		log.info("Total timeframe: " + dp.format(tsDate) + " - " + dp.format(teDate))
		
		
		
		val selexp = if(startDate != null || endDate != null) {
			if(startDate == null) startDate = new Date(0)
			if(endDate == null) endDate = new Date(Long.MaxValue)
			log.info("Limiting selection to " + dp.format(startDate) + " - " + dp.format(endDate))
			experiments.filter(x => {x.end.after(startDate) && x.end.before(endDate)})
		} else experiments
		
		
		//Output
		val configs = selexp.map(_.config.keySet).reduce(_ union _).toList.sortWith(_.compareTo(_) < 0)
		val resKeys = selexp.map(_.results.map(_.k).toSet).reduce(_ union _).toList.sortWith(_.compareTo(_) < 0)
		val nodes = selexp.map(_.results.map(_.node).toSet).reduce(_ union _).toList.sortWith(_ < _)
		val expResKeys = selexp.map(_.expResults.map(_.k).toSet).reduce(_ union _).toList.sortWith(_.compareTo(_) < 0)
		
		
		log.info("Experiments: " + selexp.size)
		log.info("Data: " + selexp.map(_.data.size).sum)
		log.info("Configs: " + configs.size)
		log.info("Keys: " + resKeys.size)
		log.info("Nodes: " + nodes.size)
		log.info("ExpResults: " + selexp.map(_.expResults.size).sum)
		log.info("ExpKeys: " + expResKeys.size);
		
		
		val (sDate, eDate) = getStartEnd(selexp)
		log.info("Timeframe: " + dp.format(sDate) + " - " + dp.format(eDate))
		//log.debug("Keys: " +  resKeys.mkString(", "))
		
		
		if(dryRun) {
			log.info("No output")
			sys.exit(0)
		}
		
		val outname = {if(outfile.length > 1) outfile else folder.toString}
		
		
		if(false) {
			//Stacked results
			log.info("Wrinting stacked results");
			;{
	
				val header = configs ::: List(nodeStr, "key", "value")
				val oLines = for(exp <- selexp) yield{
					val pres = configs.map(exp.config.getOrElse(_, "null")).mkString("", sep, sep)
					for(res <- exp.results) yield {
						pres + res.stackList.mkString(sep)
					}
				}
				if(oLines.size > 0) {
					val outfile = new java.io.PrintWriter(new File(outname + ".res.stacked"))
					outfile.println(header.mkString(sep)) 
					outfile.println(oLines.view.flatten.mkString("\n"))
					outfile.close
				} else {
					log.info("No Stacked results")
				}
			}
			
			//Stacked experiment results
			log.info("Wrinting stacked expriment results");
			;{
				val header = configs ::: List("key", "value")
				val oLines = for(exp <- selexp) yield {
					val pres = configs.map(exp.config.getOrElse(_, "null")).mkString("", sep, sep)
					val rv = for(res <- exp.expResults) yield {
						pres + res.stackList.mkString(sep)
					}
					rv
				}
				if(oLines.size > 0) {
					val outfile = new java.io.PrintWriter(new File(outname + ".res.exp_stacked"))
					outfile.println(header.mkString(sep))
					outfile.println(oLines.view.flatten.mkString("\n"))
					outfile.close
				}else {
					log.info("No Stacked experiment results")
				}
			}
		}
		//Unstacked results
		log.info("Wrinting unstacked results");
		;{
			val header =  configs ::: List(nodeStr) ::: resKeys
			val olines = for(exp <- selexp ; (node, dat) <- exp.resultsNodeKeyValueMap) yield {
					val rv:List[String] = configs.map(exp.config.getOrElse(_, "null")) ::: 
							List(node.toString)	:::
							resKeys.map(dat.getOrElse(_, "null").toString)
					if(rv.size != header.size) {
						log.error("Wrong Size!")
						log.error("RV: " + rv.size +"H: " +header.size + " C: " + configs.size + " R: " + resKeys.size)
					}
							
					rv
			}
			if(olines.size > 0) {
				val outfile = new java.io.PrintWriter(new File(outname + ".res.unstacked"))				 
				outfile.println(header.mkString(sep))
				outfile.print(olines.map(_.mkString(sep)).mkString("\n"))			
				outfile.close
			}else {
				log.info("No unstacked results")
			}
		}
		
		//Unstacked results
		log.info("Wrinting unstacked Experiment results");
		;{
			val header =  configs ::: expResKeys
			val olines = for(exp <- selexp ) yield {
					val dat = exp.expResultsKeyValueMap
					val rv = configs.map(exp.config.getOrElse(_, "null")) ::: 
							expResKeys.map(dat.getOrElse(_, "null"))
					if(rv.size != header.size) {
						log.error("Wrong Size!")
						log.error("RV"+rv.size +"H: " +header.size + " C: " + configs.size + " R: " + resKeys.size)
					}
							
					rv
			}
			if(olines.size > 0) {
				val outfile = new java.io.PrintWriter(new File(outname + ".res.exp_unstacked"))				 
				outfile.println(header.mkString(sep))
				outfile.print(olines.map(_.mkString(sep)).mkString("\n"))			
				outfile.close
			}else {
				log.info("No unstacked results")
			}
		}
		
		
		log.info("Export Information to read in R")
		;{
			val outfile = new java.io.PrintWriter(new File(outname + ".res.r_inputs"))
			outfile.println({configs ::: List(nodeStr)}.mkString(" ") )
			outfile.close
			
		}
		
		log.info("Done")
		
		
	}

}
