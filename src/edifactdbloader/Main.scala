package edifactdbloader

import scala.io.Source
import scala.xml.XML
import scala.xml.Node
import scala.collection.mutable.MutableList
import scala.collection.mutable.Map
import scala.concurrent.future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.Awaitable
import scala.concurrent.duration.Duration
import java.util.zip.GZIPInputStream
import Main.printlnDebug

object EdifactXML {

  def isDigit(c: Char) = c.toInt - 48 >= 0 && c.toInt - 48 <= 9
  var groupId = 0
  sealed abstract class Tree
  case class Segment(code: String, min: Int, max: Int, segType: String) extends Tree
  case class SegmentGroup(min: Int = 1, max: Int = 1, segments: MutableList[Tree] = MutableList(), id: Int = groupId) extends Tree {
    groupId += 1
  }
  case class Rules(segments: MutableList[Tree] = MutableList()) extends Tree {
    def print(tree: Tree = this, tab: Int = 0): Unit = tree match {
      case Rules(segs) => segs.foreach(t => print(t, tab))
      case SegmentGroup(min, max, segs, id) => {
        printlnDebug("-" * tab + "Group" + id + " " + min + ":" + max)
        segs.foreach(t => print(t, tab + 1))
      }
      case Segment(code, min, max, st) => printlnDebug("-" * tab + code + " " + min + ":" + max + { if (code != st) ":" + st else "" })
    }
  }

  case class SegmentType(code: String, var repcode: String, var minField: Int = 0, fields: MutableList[Field] = MutableList()) {
    def print = {
      printlnDebug(code)
      fields.foreach(_.print)
    }
    def isSame(segT: SegmentType): Boolean = {
      if (this.code == segT.code && this.fields.length == segT.fields.length && {
        this.fields.zip(segT.fields).foreach(e => if (!e._1.isSame(e._2)) return false)
        true
      }) true
      else false
    }
  }
  case class Field(name: String, required: Boolean, var minComp: Int = 0, components: MutableList[Component] = MutableList()) {
    def print = {
      printlnDebug("  " + name + ":" + required)
      components.foreach(c => printlnDebug("    " + c.name + ":" + c.required))
    }
    def isSame(field: Field): Boolean = {
      if (this.name == field.name && this.components.length == field.components.length && {
        this.components.zip(field.components).foreach(e => if (!e._1.isSame(e._2)) return false)
        true
      }) true
      else false
    }
  }
  case class Component(name: String, required: Boolean) {
    def isSame(comp: Component): Boolean = {
      this == comp
    }
  }

  val segmentTypes: Map[String, SegmentType] = Map()
  val segmentTypeRep: Map[String, Int] = Map()
  def createSegType(code: String, xml: Node): String = {
    val segType = new SegmentType(code, code)
    var minField = 0
    var fieldCounter = 0
    xml.child.filter(_.label != "#PCDATA").foreach(field => {
      if (field.label == "field") {
        fieldCounter += 1
        val required = (field \ "@required").text == "true"
        if (required) minField = fieldCounter
        val f = new Field((field \ "@xmltag").text, required)
        segType.fields += f
        var minComp = 0
        var compCounter = 0
        field.child.filter(_.label != "#PCDATA").foreach(comp => {
          if (comp.label == "component") {
            compCounter += 1
            val requiredComp = (comp \ "@required").text == "true"
            //printlnDebug(requiredComp)
            if (requiredComp) minComp = compCounter
            f.components += new Component((comp \ "@xmltag").text, requiredComp)
          } else throw new Exception("BAD XML - COMPONENT - " + field.label)
          if (comp.child.length != 0) throw new Exception("BAD XML - COMPONENT HAVE CHILD")
        })
        f.minComp = minComp
      } else throw new Exception("BAD XML - FIELD")
    })
    segType.minField = minField
    if (segmentTypes.contains(code)) {
      if (!segType.isSame(segmentTypes(code))) {
        segmentTypeRep(code) += 1
        val repcode = code + segmentTypeRep(code)
        segmentTypes += repcode -> segType
        segType.repcode = repcode
        //throw new Exception("Same Seg Type Code but not same descrition")
      }
    } else {
      segmentTypes += code -> segType
      segmentTypeRep += code -> 0
    }
    segType.repcode
  }

  def processXml(xmlNode: Node, tree: Tree, tab: Int = 0): Unit = {
    xmlNode.child.filter(_.label != "#PCDATA").foreach(node =>
      if (node.label == "segment") {
        val code = (node \ "@segcode").text
        val min = (node \ "@minOccurs").text.toInt
        if (min > 1) throw new Exception("Do not process minOccurs > 1")
        val max = (node \ "@maxOccurs").text.toInt
        tree match {
          case Rules(segments)                 => segments += new Segment(code, min, max, createSegType(code, node))
          //case Rules(segments)                 => segments += new Segment(code, min, max, createSegType(code, node))
          case SegmentGroup(_, _, segments, _) => segments += new Segment(code, min, max, createSegType(code, node))
          //case SegmentGroup(_, _, segments, _) => segments += new Segment(code, min, max, createSegType(code, node))
          case Segment(_, _, _, _)             => throw new Exception("Not Here SegmentGroup")
        }
      } else if (node.label == "segmentGroup") {
        val min = (node \ "@minOccurs").text.toInt
        if (min > 1) throw new Exception("Do not process minOccurs > 1")
        val max = (node \ "@maxOccurs").text.toInt
        val sg = new SegmentGroup(min, max)
        tree match {
          case Rules(segments)                 => segments += sg
          case SegmentGroup(_, _, segments, _) => segments += sg
          case Segment(_, _, _, _)             => throw new Exception("Not Here SegmentGroup")
        }
        processXml(node, sg, tab + 1)
      } else printlnDebug("." * tab + "Error " + node.label))
  }

  var rules = new Rules
  def parse(filename: String) = {
    val xml = XML.loadFile(filename)
    processXml((xml \ "segments")(0), rules)
    rules
  }

  def parseMsg(rule: Tree, msg: EdifactParser.EdifactMsg, rep: Map[Tree, Int] = Map(), tab: Int = 0): Unit = {
    if (msg.code == "UNB") msg.next
    else if (msg.code == "UNZ") msg.next
    else
      rule match {
        case Rules(seg) => seg.foreach(r => parseMsg(r, msg, rep))

        case SegmentGroup(min, max, seg, id) => {
          if (min == 1 && !rep.contains(rule)) {
            rep += rule -> 1
            printlnDebug("\t     |  " + ("- " * tab) + "In Group" + id + ":" + min + ":" + max + "(" + rep(rule) + ")")
            seg.foreach(r => parseMsg(r, msg, rep, tab + 1))
            if (max > rep(rule)) parseMsg(rule, msg, rep, tab)
          } else if (min == 0 || rep.contains(rule)) {
            val code = {
              seg(0) match {
                case Segment(code, _, _, _) => code
                case _                      => throw new Exception("Firt elem of a segmentGroup should be a segment")
              }
            }
            if (code == "_EOF_") throw new Exception("BAD PARSING FOUND _EOF_")
            if (code == msg.code) {
              if (!rep.contains(rule)) { rep += rule -> 1 } else rep(rule) += 1
              printlnDebug("\t     |  " + ("- " * tab) + "In Group" + id + ":" + min + ":" + max + " (" + rep(rule) + ")")
              seg.foreach(r => parseMsg(r, msg, rep, tab + 1))
              if (max > rep(rule)) parseMsg(rule, msg, rep, tab)
            } else { if (rep.contains(rule)) printlnDebug("\t     |  " + ("- " * tab) + "Out Group" + id + " (" + rep(rule) + ")") }
          } else throw new Exception("Not Here SegmentGroup")
        }

        case s: Segment => {
          if (msg.code == "_EOF_") throw new Exception("BAD PARSING FOUND _EOF_")
          if (s.code == msg.code) {
            if (!rep.contains(rule)) rep += rule -> 1 else rep(rule) += 1
            printlnDebug(msg.index + "\tOK   |  " + ("- " * tab) + msg.code + " -> " + s.code + ":" + s.min + ":" + s.max + "(" + rep(rule) + ")")
            printlnDebug("")
            EdifactParser.parseSegment(msg, s)
            printlnDebug("")
            msg.next
            if (s.max > rep(rule)) parseMsg(rule, msg, rep, tab)
          } else if (s.code != msg.code && s.min == 0) {
            if (!rep.contains(rule)) rep += rule -> 0
            printlnDebug(msg.index + "\tNEXT |  " + ("- " * tab) + msg.code + " -> " + s.code + ":" + s.min + ":" + s.max + "(" + rep(rule) + ")")
          } else throw new Exception("Not Here Segment")
        }
      }
  }
}

// read line with custom separator
object EdifactSource {
  var sSep = '\''
  class SegmentIterator(it: Iterator[Char]) extends Iterator[String] {
    private val sb = new StringBuilder
    def hasNext = it.hasNext
    def next = {
      sb.clear()
      var c = '\0'
      while (it.hasNext && { c = it.next(); c } != sSep) sb.append(c)
      sb.toString
    }
  }
  def getFromFile(name: String) = new SegmentIterator(Source.fromFile(name))
  def getStdin = new SegmentIterator(Source.stdin)
  def getIterator(it: Iterator[Char]) = new SegmentIterator(it)
}

object EdifactParser {
  val codes = """
  
FDR FDD REF STX LTS IFD APD DAT STX EQP EQI EQD IMD DAT SSR EQN SCI SDT LEG EQN
EQI CBL EQN CBA BPR BUC EQN ATC ODI DAT CAR TRF STX SCI EQN TBU PDI CLA EQN CAR
EQN SBI EQN SBC MON ATC EQN ATI RCI CAR EQN STX TRA SDT DAT              

""".replace('\n', ' ').split(' ').filter(!_.isEmpty)

  var msgNbr = 0

  var fSep = '+'
  var cSep = ':'

  class EdifactMsg(val segs: List[String]) {
    var index = 0
    val allFields = segs(index).split(fSep).splitAt(1)
    var code = allFields._1(0)
    var fields = allFields._2
    var segment = segs(index)
    msgNbr += 1
    if (msgNbr % 100 == 0) System.err.println(msgNbr)
    override def toString = code + ":" + segs.length.toString + ":" + segs.last.split(fSep)(0)
    def next = {
      index += 1
      code = if (index != segs.length) {
        val allFields = segs(index).split(fSep).splitAt(1)
        fields = allFields._2
        segment = segs(index)
        allFields._1(0)
      } else "_EOF_"
    }
  }

  def parseField(field: String, rule: EdifactXML.Field) = {
    val comps = field.split(EdifactParser.cSep)
    printlnDebug("COMP: lenght found:" + comps.length + " min:" + rule.minComp + " max:" + rule.components.length)
    //if (comps.length < rule.minComp || comps.length > rule.components.length) throw new Exception("Component field number error")
    if (comps.length < rule.minComp || comps.length > rule.components.length) printlnDebug("Component field number error")
    for (i <- 0 to comps.length - 1) {
      if (rule.components.isDefinedAt(i))
        printlnDebug("  Comp(" + i + ") " + rule.components(i).name + " -> " + comps(i))
      else
        printlnDebug("  Comp(" + i + ") -> not defined")
    }
  }

  def parseSegment(msg: EdifactMsg, rule: EdifactXML.Segment) = {
    val fields = msg.fields
    printlnDebug(msg.segment)
    val segType = EdifactXML.segmentTypes(rule.segType)
    printlnDebug("SEG: lenght found:" + fields.length + " min:" + segType.minField + " max:" + segType.fields.length)
    if (fields.length < segType.minField || fields.length > segType.fields.length) throw new Exception("Segment field number error")
    for (i <- 0 to fields.length - 1) {
      printlnDebug("Field(" + i + ") " + segType.fields(i).name + " -> " + fields(i))
      if (fields(i).contains(EdifactParser.cSep)) parseField(fields(i), segType.fields(i))
    }
  }

  def getEmptyMsg = new EdifactMsg(List())

  def getEdifactMsg(it: Iterator[String]) = new Iterator[EdifactMsg] {
    private var isFirst = true;
    private var segNbr = 0;
    def hasNext = it.hasNext

    def next = {
      var seg = it.next()
      var code = seg.split(fSep)(0)
      segNbr += 1

      if (code == "UNB" && isFirst) {
        isFirst = false
        new EdifactMsg(List(seg))
      } else if (code == "UNZ" && !isFirst) {
        isFirst = true
        new EdifactMsg(List(seg))
      } else if (code == "UNH" && !isFirst) {
        var segs = List(seg)
        while (it.hasNext && { segNbr += 1; seg = it.next(); code = seg.split(fSep)(0); code } != "UNT") {
          if (code == "UNH") throw new Exception("msg no closed, line " + segNbr)
          if (!codes.contains(code)) throw new Exception("code " + code + " not know, line" + segNbr)
          segs = segs :+ seg
        }
        segs = segs :+ seg
        new EdifactMsg(segs)
      } else {
        throw new Exception("parsing failed [" + segNbr.toString + ", " + code + ", " + seg + "]")
      }

    }

  }
}

object Main {
  //val segSeparator = '\34'
  //val elemSeparator = '\35'
  //val comSeparator = '\37'
  EdifactSource.sSep = '\34'
  EdifactParser.fSep = '\35'
  EdifactParser.cSep = '\37'
  //EdifactSource.sSep = '\n'
  //EdifactParser.fSep = '+'
  //EdifactParser.cSep = ':'
  
  var debug = false

  def parseMsg(rules: EdifactXML.Rules, msg: EdifactParser.EdifactMsg) = {
    EdifactXML.parseMsg(rules, msg)
    printlnDebug(msg.code)
    if (msg.code != "_EOF_") throw new Exception("BAD PARSING END")
  }

  def printlnDebug(log: String) = if(debug) println(log)

  def main(args: Array[String]): Unit = {
    var workers = 2
    var takeMsg = 3000
    if (args.length == 1) workers = args(0).toInt
    if (args.length == 2) {
      workers = args(0).toInt
      takeMsg = args(1).toInt
    }
    val rules = EdifactXML.parse("edimappingconfig.xml")
    //rules.print()
    //System.exit(0)
    //EdifactXML.segmentTypes.foreach(e => { print(e._1 + " => "); e._2.print })
    //val is = new GZIPInputStream(System.in)
    val it = EdifactParser.getEdifactMsg(EdifactSource.getStdin)
    //val it = EdifactParser.getEdifactMsg(EdifactSource.getIterator(Source.createBufferedSource(is)))
    it.foreach(parseMsg(rules, _))
    //is.close
    //it.next
    //parseMsg(rules, it.next)

    /*
    val it = EdifactParser.getEdifactMsg(EdifactSource.getStdin)
    var wks: List[Awaitable[Unit]] = List()
    for (i <- 1 to workers) {
      System.err.println("Create worker " + i + " , " + takeMsg + " messages")
      val f = future {
        var continue = true
        while (continue) {
          var msgList: MutableList[EdifactParser.EdifactMsg] = MutableList()
          var j = 0
          it.synchronized {
            while (it.hasNext && j < takeMsg) {
              msgList += it.next
              j += 1
            }
          }
          if (j < takeMsg) continue = false
          msgList.foreach(parseMsg(rules, _))
        }
      }
      wks = wks :+ f
      Thread.sleep(5000)
    }
    wks.foreach(Await.result(_, Duration.Inf))
    */
    println("\n" + EdifactParser.msgNbr + " messages parsed.")
  }
}
