<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

  <xsl:import href="breadcrumbs.xsl"/>
  
<xsl:template name="user.head.content">
  <xsl:text disable-output-escaping="yes">
<![CDATA[

<!-- favicon -->

<link rel="shortcut icon" href="http://neo4j.org/favicon.ico" type="image/vnd.microsoft.icon" />
<link rel="icon" href="http://neo4j.org/favicon.ico" type="image/x-icon" />

<!-- style -->

<link href="css/shCore.css" rel="stylesheet" type="text/css" />
<link href="css/shCoreEclipse.css" rel="stylesheet" type="text/css" />
<link href="css/shThemeEclipse.css" rel="stylesheet" type="text/css" />
<link href="css/neo.css" rel="stylesheet" type="text/css" />

<!-- JQuery -->

<script type="text/javascript" src="js/jquery-1.6.4.min.js"></script>
<script type="text/javascript" src="js/jquery-ui-1.10.3.custom.min.js"></script>

<!-- Replace SVG for browsers that lack support. -->
<script type="text/javascript" src="js/svgreplacer.js"></script>

<!-- Image Scaler -->

<script type="text/javascript" src="js/imagescaler.js"></script>

<!-- Table Styler -->

<script type="text/javascript" src="js/tablestyler.js"></script>

<!-- Version -->

<script type="text/javascript" src="js/version.js"></script>

<!-- Version Switcher -->

<script type="text/javascript" src="js/versionswitcher.js"></script>

<!-- Cypher Console -->

<link href="http://netdna.bootstrapcdn.com/font-awesome/3.2.1/css/font-awesome.min.css" rel="stylesheet">
<script type="text/javascript" src="js/console.js"></script>
<script type="text/javascript" src="js/cypherconsole.js"></script>

<!-- Syntax Highlighter -->

<script type="text/javascript" src="js/shCore.js"></script>
<script type="text/javascript" src="js/shBrushJava.js"></script>
<script type="text/javascript" src="js/shBrushJScript.js"></script>
<script type="text/javascript" src="js/shBrushBash.js"></script>
<script type="text/javascript" src="js/shBrushPlain.js"></script>
<script type="text/javascript" src="js/shBrushXml.js"></script>
<script type="text/javascript" src="js/shBrushGroovy.js"></script>
<script type="text/javascript" src="js/shBrushCypher.js"></script>
<script type="text/javascript" src="js/shBrushScala.js"></script>
<script type="text/javascript" src="js/shBrushSql.js"></script>
<script type="text/javascript" src="js/shBrushPython.js"></script>
<script type="text/javascript" src="js/shBrushProperties.js"></script>

<!-- activate when needed
<script type="text/javascript" src="js/shBrushRuby.js"></script>
<script type="text/javascript" src="js/shBrushCSharp.js"></script>
-->
 
<script type="text/javascript">
  SyntaxHighlighter.defaults['tab-size'] = 4;
  SyntaxHighlighter.defaults['gutter'] = false;
  SyntaxHighlighter.defaults['toolbar'] = false;
  SyntaxHighlighter.all()
</script>

<!-- Online Sidebar -->

<script type="text/javascript" src="../sidebar.js"></script>

]]>
  </xsl:text>
  
  <xsl:call-template name="breadcrumbs"/>
  
</xsl:template>

</xsl:stylesheet>

