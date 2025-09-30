<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output omit-xml-declaration="yes" indent="yes"/>
    <xsl:strip-space elements="*"/>

    <!-- identity template -->
    <xsl:template match="node()|@*">
        <xsl:copy>
            <xsl:apply-templates select="node()|@*"/>
        </xsl:copy>
    </xsl:template>

    <!-- Move <value> child to @value on any element except <offset> -->
    <xsl:template match="*[not(self::offset) and value]">
        <xsl:copy>
            <!-- copy all attributes except an existing @value -->
            <xsl:apply-templates select="@*[name()!='value']"/>
            <!-- add/replace the value attribute -->
            <xsl:attribute name="value">
                <xsl:value-of select="value"/>
            </xsl:attribute>
            <!-- copy child nodes (identity template will remove the <value> via the next rule) -->
            <xsl:apply-templates select="node()"/>
        </xsl:copy>
    </xsl:template>

    <!-- Suppress the <value> element unless its parent is <offset> -->
    <xsl:template match="value[not(parent::offset)]"/>
</xsl:stylesheet>