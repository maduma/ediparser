/*
 * stephane.nsakala@luxairgroup.lu - 2014
 */

package edifactdbloader.parser

import edifactdbloader.tree.ETree
import edifactdbloader.tree.XMLParser
import edifactdbloader.tree.XMLParser.ETerm
import edifactdbloader.tree.Counter
import java.security.MessageDigest
import scala.io.Source
import scala.xml.Node
import scala.xml.XML
import EDIParser._

class EDIRule() {
  override def toString = "RULE"
}

class EDIRuleSeg(node: Node) {
  val code = (node \ ("@" + segcodeAtt)).text
  val tag = (node \ ("@" + tagAtt)).text
  val min = (node \ ("@" + minAtt)).text.toInt
  val max = (node \ ("@" + maxAtt)).text.toInt
  var minField = 0
  var maxField = (node \ fieldTag).length

  (node \ fieldTag).foreach(n => { if (n.attributes(requiredAtt).text == "true") minField += 1 })

  if (!code.matches("[A-Z]{3}")) throw new Exception("Not a valid code")
  if (!tag.matches("[\\w-]+")) throw new Exception("Not a valid tag")
  if (min < 0 || max < 0) throw new Exception("min or max < 0 " + code)
  if (min > max) throw new Exception("min > max " + code)
  if (min > 1) throw new Exception("min > 1 not implemented " + code)

  override def toString = Array(code, min, max, minField, maxField).mkString(":")
}

class EDIRuleSegGroup(node: Node, id: Int) {
  val code = "GROUP" + id
  val tag = (node \ ("@" + tagAtt)).text
  val min = (node \ ("@" + minAtt)).text.toInt
  val max = (node \ ("@" + maxAtt)).text.toInt

  if (!tag.matches("[\\w-]+")) throw new Exception("Not a valid tag")
  if (min < 0 || max < 0) new Exception("min or max < 0")
  if (min > max) throw new Exception("min > max")
  if (min > 1) throw new Exception("min > 1 not implemented")

  override def toString = Array(code, min, max).mkString(":")
}

class EDIRuleComponent(node: Node) {
  val required = (node \ ("@" + requiredAtt)).text == "true"
  val tag = (node \ ("@" + tagAtt)).text

  if (!tag.matches("[\\w-]+")) throw new Exception("Not a valid tag")

  override def toString = if (printField) Array(tag, required).mkString(":") else ""
}

class EDIRuleField(node: Node) extends EDIRuleComponent(node) {
  var maxComponent = (node \ componentTag).length
  var minComponent = 0
  (node \ componentTag).foreach(n => { if (n.attributes(requiredAtt).text == "true") minComponent += 1 })
  override def toString = if (printField) Array(tag, required, minComponent, maxComponent).mkString(":") else ""
}

class Result(tag: String, value: String = "", child: Array[Result] = Array()) {
  override def toString =
    if (child.length == 0) "<" + tag + ">" + value + "</" + tag + ">"
    else "<" + tag + ">" + child.mkString("") + "</" + tag + ">"
}

class EDIParser {
  var codes: Set[String] = Set()
  var rootRule = ETerm

  def ruleFromXML(xmlNode: Node) = ruleFromXML_SegmentsNode((xmlNode \ segmentsTag)(0))

  def ruleFromXML_SegmentsNode(xmlNode: Node): ETree = {
    val parser = new XMLParser()
    var groupId = new Counter()
    rootRule = parser.fromXML(xmlNode, f = node => {
      node.label match {
        case `segmentsTag`     => new EDIRule
        case `segmentGroupTag` => new EDIRuleSegGroup(node, groupId.++)
        case `segmentTag`      => { val seg = new EDIRuleSeg(node); codes = codes + seg.code; seg }
        case `fieldTag`        => new EDIRuleField(node)
        case `componentTag`    => new EDIRuleComponent(node)
        case _                 => throw new Exception("XML not an edifcat rule")
      }
    });
    rootRule
  }

  def parseComponent(comp: String, rTree: ETree): Result = rTree.data match {
    case r: EDIRuleComponent => {
      printlnDebug(r.tag + " -> " + comp)
      new Result(r.tag, comp)
    }
    case _ => throw new Exception("Cannot get component tag")
  }

  def parseField(field: String, rTree: ETree): Result = rTree.data match {
    case r: EDIRuleField => {
      val comps = field.split(cSep)
      printlnDebug("- cols: " + comps.length + " " + r.minComponent + ":" + r.maxComponent + ", " + r.tag)

      if (comps.length < r.minComponent) throw new Exception("not enouth Components")
      if (comps.length > r.maxComponent) throw new Exception("too many Components")

      var rule = rTree.child
      var results = Array[Result]()
      comps.foreach(comp => {
        if (comp != "") results = results :+ parseComponent(comp, rule)
        else rule.data match {
          case r: EDIRuleComponent => printlnDebug(r.tag + " -|")
          case _                   => throw new Exception("Cannot get component tag")
        }
        rule = rule.sibling
      })
      new Result(r.tag, child = results)
    }
    case _ => throw new Exception("Cannot get minComponents")
  }

  def parseSeg(seg: EDISeg, rTree: ETree): Result = rTree.data match {
    case r: EDIRuleSeg =>
      {
        printlnDebug("- flds: " + seg.fields.length)

        if (seg.fields.length < r.minField) throw new Exception("not enouth Field")
        if (seg.fields.length > r.maxField) throw new Exception("too many Field")

        var rule = rTree.child
        var index = 0
        var results = Array[Result]()
        seg.fields.foreach(field => {
          printlnDebug("[" + index + "]")
          if (field.contains(cSep)) results = results :+ parseField(field, rule)
          else {
            val tag = rule.data match {
              case rf: EDIRuleField => rf.tag
              case _                => throw new Exception("Cannot get Field Tag")
            }
            if (field != "") {
              results = results :+ new Result(tag, field)
              printlnDebug("- cols: 1, " + tag + " -> " + field)
            } else printlnDebug("- cols: 0, " + tag + " -|")
          }
          index += 1
          rule = rule.sibling
        })
        new Result(r.tag, child = results)
      }
    case _ => throw new Exception("Cannot get minField")
  }

  def parseMsg(msg: EDIMsg, rTree: ETree = rootRule): Result = {
    val results = parseMsgImp(msg, rTree)
    if (!Array("UNB", "UNZ", "UNT").contains(msg.last.code)) throw new Exception("last seg is not UNB,UNZ or UNT")
    printlnDebug("#" * 80)
    if (results.length == 0) new Result("MESSAGE", msg.toString)
    else if (results.length == 1) results.last
    else throw new Exception("More than one result in results array")
  }

  def parseMsgImp(msg: EDIMsg, rTree: ETree = rootRule, rep: Array[Int] = Array.fill(rootRule.id + 1)(0), results: Array[Result] = Array()): Array[Result] = {
    if (msg.hasNext && rTree != ETerm) {
      var seg = msg.seg
      def parseAndNextSeg = { printlnDebug(seg.code); val result = parseSeg(seg, rTree); msg.incSeg; result }
      def nextRuleOrAgain(max: Int) = if (rep(rTree.id) == max) { rstRule; parseMsgImp(msg, rTree.sibling, rep, results) } else parseMsgImp(msg, rTree, rep, results)
      def incRule = rep(rTree.id) += 1
      def rstRule = rep(rTree.id) = 0
      def nextRule = {
        val seg = rTree.child.data match {
          case nc: EDIRuleSeg      => nc.code
          case nc: EDIRuleSegGroup => nc.code
          case _                   => throw new Exception("next code error")
        }
        printlnDebug("- nseg: " + seg)
        seg
      }
      if (seg.code != "UNB" && seg.code != "UNZ") {
        printlnDebug("-" * 40)
        printlnDebug("-  seg: " + seg)
        printlnDebug("- rule: " + rTree.data.getClass.getSimpleName + ", " + rTree.data + ", id" + rTree.id + ":" + rep(rTree.id))
        rTree.data match {
          case r: EDIRule => { Array(new Result("MESSAGE", child = parseMsgImp(msg, rTree.child, rep, results))) }
          case r: EDIRuleSeg =>
            if (rep(rTree.id) < r.min) {
              if (seg.code == r.code) { val segResult = parseAndNextSeg; incRule; segResult +: nextRuleOrAgain(r.max) }
              else throw new Exception("parse msg error " + r.code + " code not found")
            } else {
              if (seg.code == r.code) { val segResult = parseAndNextSeg; incRule; segResult +: nextRuleOrAgain(r.max) }
              else { rstRule; parseMsgImp(msg, rTree.sibling, rep, results) }
            }
          case r: EDIRuleSegGroup =>
            if (rep(rTree.id) < r.min) { incRule; val childResult = parseMsgImp(msg, rTree.child, rep, results); new Result(r.tag, child = childResult) +: nextRuleOrAgain(r.max) }
            else {
              if (seg.code == nextRule) { incRule; val childResult = parseMsgImp(msg, rTree.child, rep, results); new Result(r.tag, child = childResult) +: nextRuleOrAgain(r.max) }
              else { rstRule; parseMsgImp(msg, rTree.sibling, rep, results) }
            }
          case r: EDIRuleField     => results
          case r: EDIRuleComponent => results
          case _                   => throw new Exception("bad rule type")
        }
      } else results
    } else results
  }
}

class EDIMsgIterator(it: EDIRawSegIterator, cksum: EDICksum = new EDICksum, onlyNew: Boolean = false) extends Iterator[EDIMsg] {
  var msgNbr = 0
  var newMsgNbr = 0

  if (onlyNew && cksum.length == 0) throw new Exception("onlyNew True but cksum empty")

  private var buf = new EDIMsg
  private var isFirst = true

  def hasNext = if (onlyNew) hasNextNewMsg else hasNextMsg
  def next = if (onlyNew) nextNewMsg else nextMsg
  def nextNewMsg = { newMsgNbr += 1; buf }
  def hasNextNewMsg = it.hasNext && {
    buf = nextMsg
    var findNew = false
    do {
      findNew = !cksum.contains(buf)
      if (!findNew && it.hasNext) buf = nextMsg
    } while (!findNew && it.hasNext)
    findNew
  }
  def hasNextMsg = it.hasNext
  def nextMsg = {
    msgNbr += 1;
    if (msgNbr % 100 == 0) System.err.print(msgNbr + "\r")
    val rawSeg = it.next
    var parts = rawSeg.split(fSep).splitAt(1)
    var code = parts._1(0)
    if (code == "UNB" && isFirst) {
      isFirst = false
      new EDIMsg(Array(new EDISeg(code, parts._2)), rawSeg)
    } else if (code == "UNZ" && !isFirst) {
      isFirst = true
      new EDIMsg(Array(new EDISeg(code, parts._2)), rawSeg)
    } else if (code == "UNH" && !isFirst) {
      val sb = new StringBuilder(rawSeg)
      var segs = Array(new EDISeg(code, parts._2))
      while (it.hasNext && {
        val rawSeg = it.next
        sb.append(rawSeg)
        parts = rawSeg.split(fSep).splitAt(1)
        code = parts._1(0)
        code
      } != "UNT") {
        if (code == "UNH") throw new Exception("msg no closed")
        segs = segs :+ new EDISeg(code, parts._2)
      }
      segs = segs :+ new EDISeg(code, parts._2)
      new EDIMsg(segs, sb.toString)
    } else throw new Exception("parsing failed")
  }
}

class EDIRawSegIterator(it: Iterator[Char]) extends Iterator[String] {
  private val sb = new StringBuilder
  def hasNext = it.hasNext
  def next = {
    sb.clear()
    var c = '\0'
    while (it.hasNext && { c = it.next(); c } != sSep) sb.append(c)
    sb.toString
  }
}

class EDISeg(val code: String = "", val fields: Array[String] = Array()) {
  override def toString = (code +: fields).mkString(fSep.toString)
}

class EDIMsg(segs: Array[EDISeg] = Array(), raw: String = "") {
  val uuid = md5(raw) + raw.size
  private var index = 0

  def seg = segs(index)
  def incSeg = index += 1
  def hasNext = index < segs.length
  def next = { val seg = segs(index); index += 1; seg }
  def last = if (index > 0) segs(index - 1) else segs(0)
  def length = segs.length

  def toStringNoCksum = segs.mkString(sSep.toString)
  def toStringUUID = "" +
    "=============================================================\n" +
    "cksum: " + uuid + "\n" +
    "-------------------------------------------------------------\n" +
    segs.mkString(sSep.toString)
  override def toString = segs.mkString(sSep.toString)
}

class EDIEndMsg extends EDIMsg

class EDICksum extends Iterator[String] {
  private var index = 0
  var cksums = Set[String]()
  private var cksumsIt = cksums.iterator

  def load(it: Iterator[String]) = {
    it.foreach({ cksum => cksums = cksums + cksum })
    cksumsIt = cksums.iterator
    this
  }

  def contains(msg: EDIMsg) = cksums(msg.uuid)
  override def length = cksums.size
  def hasNext = cksumsIt.hasNext
  def next = cksumsIt.next
}

object EDIParser {
  val segmentsTag = "segments"
  val segmentGroupTag = "segmentGroup"
  val segmentTag = "segment"
  val fieldTag = "field"
  val componentTag = "component"

  val segcodeAtt = "segcode"
  val minAtt = "minOccurs"
  val maxAtt = "maxOccurs"
  val requiredAtt = "required"
  val tagAtt = "xmltag"

  //val sSep = '\n'
  //val fSep = '+'
  //val cSep = ':'

  val sSep = '\34'
  val fSep = '\35'
  val cSep = '\37'

  var debug = false
  val printField = true

  val messageDigest = MessageDigest.getInstance("MD5")

  def md5(s: String) = {
    messageDigest.digest(s.getBytes).map("%02X" format _).mkString
  }

  def generateCksum(in: Iterator[Char]) = {
    new EDIMsgIterator(new EDIRawSegIterator(in))
      .foreach(m => println(m.uuid))
  }

  def printlnDebug(log: String) = if (debug) println(log)

  def main(args: Array[String]): Unit = {

    val parser = new EDIParser()
    val rule = parser.ruleFromXML(XML.loadFile(args(0)))
    //println(rule)
    //System.exit(0)

    debug = (System.getProperty("debug") == "true")
    if (debug) System.err.println("Debug enabled.")
    val doChecksum = (System.getProperty("doCksum") == "true")

    val msgIt = {
      if (doChecksum) {
        System.err.println("cksum enabled.")
        val cksumFileName = System.getProperty("cksumFile")
        val cksum = new EDICksum().load(Source.fromFile(cksumFileName).getLines)
        new EDIMsgIterator(new EDIRawSegIterator(Source.stdin), cksum, doChecksum)
      } else new EDIMsgIterator(new EDIRawSegIterator(Source.stdin))
    }
    println("<RESULT>")
    msgIt.foreach(msg => {
      val result = parser.parseMsg(msg)
      println(result)
    })
    println("</RESULT>")

    System.err.println(msgIt.msgNbr + " messages.")
    if (doChecksum) System.err.println(msgIt.newMsgNbr + " new messages.")

    //generateCksum(Source.stdin)
  }
}
