<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output omit-xml-declaration="yes" indent="yes"/>
	<xsl:strip-space elements="*"/>
	<xsl:template match="node()|@*" name="identity">
		<xsl:copy>
			<xsl:apply-templates select="node()|@*"/>
		</xsl:copy>
	</xsl:template>




	<!--	match anything that has a child that is position and move position to an attribute -->
	<xsl:template match="*[./position]">
		<xsl:copy>
			<xsl:attribute name='position'>
				<xsl:value-of select="./position"/>
			</xsl:attribute>
			<xsl:apply-templates/>
		</xsl:copy>
	</xsl:template>

	<!--	drop position elements-->
	<xsl:template match="position"/>

	<!--	drop noNamespaceSchemaLocation elements-->
	<xsl:template match="noNamespaceSchemaLocation"/>


</xsl:stylesheet>