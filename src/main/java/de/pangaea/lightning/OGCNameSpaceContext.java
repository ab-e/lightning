package de.pangaea.lightning;

import java.util.Iterator;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;

public class OGCNameSpaceContext implements NamespaceContext {
 
    @Override
    public String getNamespaceURI(String prefix) {
    	if (prefix == null) {
            throw new IllegalArgumentException("No prefix provided!");
        } else if (prefix.equals(XMLConstants.DEFAULT_NS_PREFIX)) {
            return "http://www.opengis.net/sensorml/2.0";
        } else if (prefix.equals("sml")) {
            return "http://www.opengis.net/sensorml/2.0";
        } else if (prefix.equals("gml")) {
            return "http://www.opengis.net/gml/3.2";
        } else if (prefix.equals("swe")) {
            return "http://www.opengis.net/swe/2.0";
    	} else if (prefix.equals("gmd")) {
    		return "http://www.isotc211.org/2005/gmd";
    	} else {
            return XMLConstants.NULL_NS_URI;
        }
    }
 
    @Override
    public String getPrefix(String namespaceURI) {
        return null;
    }
 
    @SuppressWarnings("rawtypes")
    @Override
    public Iterator getPrefixes(String namespaceURI) {
        return null;
    }
 
}
