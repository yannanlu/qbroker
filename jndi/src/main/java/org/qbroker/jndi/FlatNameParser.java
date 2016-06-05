package org.qbroker.jndi;

/* FlatNameParser - NameParser based on flat XML Context */

import javax.naming.NameParser;
import javax.naming.Name;
import javax.naming.CompoundName;
import javax.naming.NamingException;
import java.util.Properties;

public class FlatNameParser implements NameParser {
    static Properties syntax = new Properties();

    public Name parse(String name) throws NamingException {
        return new CompoundName(name, syntax);
    }

    static {
        syntax.put("jndi.syntax.direction", "flat");
        syntax.put("jndi.syntax.ignorecase", "false");
    }
}
