<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output omit-xml-declaration="yes" indent="yes"/>
	<xsl:strip-space elements="*"/>
	<xsl:template match="node()|@*" name="identity">
		<xsl:copy>
			<xsl:apply-templates select="node()|@*"/>
		</xsl:copy>
	</xsl:template>


	<!--	match anything that has a child that is office-id and move office-id to an attribute -->
	<xsl:template match="*[./office-id]">
		<xsl:copy>
			<xsl:attribute name='office-id'>
				<xsl:value-of select="./office-id"/>
			</xsl:attribute>
			<xsl:apply-templates/>
		</xsl:copy>
	</xsl:template>

	<!--	drop office-id elements-->
	<xsl:template match="office-id"/>

	<!--	drop noNamespaceSchemaLocation elements-->
	<xsl:template match="noNamespaceSchemaLocation"/>


</xsl:stylesheet>