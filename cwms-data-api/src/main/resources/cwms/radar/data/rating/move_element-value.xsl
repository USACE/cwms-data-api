<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output omit-xml-declaration="yes" indent="yes"/>
	<xsl:strip-space elements="*"/>
	<xsl:template match="node()|@*" name="identity">
		<xsl:copy>
			<xsl:apply-templates select="node()|@*"/>
		</xsl:copy>
	</xsl:template>


	<!-- move contents of <element-value>1231231</element-value> into parent -->
	<xsl:template match="*[./element-value]">
		<xsl:copy>
			<xsl:apply-templates select="@*"/>
			<xsl:value-of select="./element-value"/>
		</xsl:copy>
	</xsl:template>
	<!--	drop element-value elements-->
	<xsl:template match="element-value"/>

</xsl:stylesheet>