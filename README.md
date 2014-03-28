EDIFACT file parser writen in Scala
===================================

Compilation
-----------

scalac src/edifactdbloader/tree/XMLParser.scala  
scalac src/edifactdbloader/parser/EDIParser.scala  

Run
---

- mapping file givent as argument
- EDIFACT is read from stdin

    gzcat UAT.NGI.BIF.INV.D140319.T130007.AMA.LGL.FTP.DATA.gz | \  
    scala edifactdbloader.parser.EDIParser edimappingconfig.xml

# options
- -Ddebug=true
