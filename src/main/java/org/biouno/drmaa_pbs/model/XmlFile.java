package org.biouno.drmaa_pbs.model;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.ConversionException;
import com.thoughtworks.xstream.io.StreamException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Inspired in Jenkins' XmlFile. Responsible for reading and writing XML files. Used for parsing PBS server qstat output
 * and for unit tests.
 *
 * @author Bruno P. Kinoshita
 * @since 0.3
 */
public class XmlFile {

    private final XStream xstream;

    private static final XStream DEFAULT_XSTREAM = new XStream();

    private static final Logger LOGGER = Logger.getLogger(XmlFile.class.getName());

    public XmlFile() {
        this(DEFAULT_XSTREAM);
    }

    public XmlFile(XStream xstream) {
        this.xstream = xstream;
    }

    @NonNull
    public XStream getXStream() {
        return this.xstream;
    }

    public Object read(File file) throws IOException {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Reading " + file);
        }
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        try {
            return getXStream().fromXML(in);
        } catch (StreamException e) {
            throw new IOException("Unable to read " + file, e);
        } catch (ConversionException e) {
            throw new IOException("Unable to read " + file, e);
        } catch (Error e) {// mostly reflection errors
            throw new IOException("Unable to read " + file, e);
        } finally {
            in.close();
        }
    }

    public Object read(String string) throws IOException {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine(String.format("Reading string [%s]", string));
        }
        try {
            return getXStream().fromXML(string);
        } catch (StreamException e) {
            throw new IOException(String.format("Unable to read from string [%s]", string), e);
        } catch (ConversionException e) {
            throw new IOException(String.format("Unable to read from string [%s]", string), e);
        }
    }
}
