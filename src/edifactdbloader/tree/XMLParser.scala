/*
 * stephane.nsakala@luxairgroup.lu - 2014
 */

package edifactdbloader.tree

import scala.xml.XML
import scala.xml.pull._
import scala.xml.Node
import XMLParser._

abstract class ETree(val child: ETree = ETerm, val sibling: ETree = ETerm, val id: Int = 0, val depth: Int = 0, val data: Any = 0) {
  override def toString = toStringImpl.mkString(nodeSep.toString)
  def toStringAllImpl = Array[String]()
  def toStringImpl = Array[String]()
}

class ENode(child: ETree, sibling: ETree, id: Int, depth: Int, data: Any) extends ETree(child, sibling, id, depth, data) {
  override def toStringAllImpl = {
    var meIn = tabStr * depth + f"$id%4d" + grpIn + dataSep + data
    var meOut = tabStr * depth + f"$id%4d" + grpOut
    (meIn +: child.toStringAllImpl :+ meOut) ++ {
      if (sibling != ETerm) sibling.toStringAllImpl else Array[String]()
    }
  }
  override def toStringImpl = {
    var meIn = tabStr * depth + f"$id%4d" + grpIn + dataSep + data
    var meOut = tabStr * depth + f"$id%4d" + grpOut
    meIn +: child.toStringAllImpl :+ meOut
  }
}

class ELeaf(sibling: ETree, id: Int, depth: Int, data: Any) extends ETree(ETerm, sibling, id, depth, data) {
  override def toStringAllImpl = {
    var me = tabStr * depth + f"$id%4d" + dataSep + data
    me +: { if (sibling != ETerm) sibling.toStringAllImpl else Array[String]() }
  }
  override def toStringImpl = {
    Array(tabStr * depth + f"$id%4d" + dataSep + data)
  }
}

class Counter {
  var id = 0
  def ++ = { id += 1; id - 1 }
  def -- = { id -= 1; id + 1 }
}

class XMLParser {
  def fromXML(xmlNode: Node, sibling: ETree = ETerm, id: Counter = new Counter(), depth: Counter = new Counter(), f: Node => Any = f => 0): ETree = {
    if (xmlNode.child.filter(_.label != "#PCDATA").length == 0) {
      new ELeaf(sibling, id.++, depth.id, f(xmlNode))
    } else {
      depth.++
      val child = xmlNode.child.filter(_.label != "#PCDATA").foldRight(ETerm) {
        (xmlNode, sibling) => fromXML(xmlNode, sibling, id, depth, f)
      }
      depth.--
      new ENode(child, sibling, id.++, depth.id, f(xmlNode))
    }
  }
}

// TODO Stream parser (no all xml in memory)
class XMLEventParser {
  def fromXML(reader: XMLEventReader, sibling: ETree = ETerm, id: Counter = new Counter(), depth: Counter = new Counter(), f: Node => Any = f => 0): ETree = {
    if (reader.hasNext) {
      reader.next match {
        case EvElemStart(_, _, _, _) => 1
        case EvElemEnd(_, _)         => 2
        case _                       => 3
      }
    }
    ETerm
  }
}

object XMLParser {
  var tabStr = "  "
  var nodeSep = "\n"
  var dataSep = " "
  var grpIn = "("
  var grpOut = ")"

  val ETerm = new ETree { override def toStringImpl = Array("ETree Terminaison") }

  def main(args: Array[String]): Unit = {

    // Test Case
    val parser = new XMLParser()
    val tree = parser.fromXML(XML.loadString(
      """
     <segmentGroup name="cars">
       <segment name="honda"></segment>
       <segment name="bmw"></segment>
       <segment name="audi"></segment>
       <segment name="ferrari"></segment>
       <segmentGroup name="model">
         <segment name="testarossa"></segment>
         <segment name="mondial"></segment>
         <segmentGroup name="year">
           <segment name="1975"></segment>
           <segment name="1980"></segment>
         </segmentGroup>
       </segmentGroup>
       <segment name="citroen"></segment>
     </segmentGroup>      
     """), f = node => {
      node \ "@name"
    })
    nodeSep = " "
    val correct = "  11( cars     10 honda      9 bmw      8 audi" +
      "      7 ferrari      6( model        5 testarossa        4 mondial" +
      "        3( year          2 1975          1 1980        3)      6)" +
      "      0 citroen   11)"
    println("Test: " + { if (tree.toString == correct) "success" else "failure" })
    nodeSep = "\n"
    println(tree)
  }
}

