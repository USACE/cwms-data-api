<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output omit-xml-declaration="yes" indent="yes"/>
	<xsl:strip-space elements="*"/>
	<xsl:template match="node()|@*" name="identity">
		<xsl:copy>
			<xsl:apply-templates select="node()|@*"/>
		</xsl:copy>
	</xsl:template>

	<!--	match anything that has a child that is value and move value to an attribute -->
	<xsl:template match="*[name() != 'offset' and ./value]">
		<xsl:copy>
			<xsl:attribute name='value'>
				<xsl:value-of select="./value"/>
			</xsl:attribute>
			<xsl:apply-templates/>
		</xsl:copy>
	</xsl:template>

	<!-- find elements named 'value' whose parent is not named 'offset' -->
	<xsl:template match="value[not(parent::offset)]"/>



</xsl:stylesheet>