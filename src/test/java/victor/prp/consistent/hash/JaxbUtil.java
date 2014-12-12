package victor.prp.consistent.hash;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @author victorp
 */
public class JaxbUtil {
    private JaxbUtil() {
    }

    public static <T> T unmarshal(File file,Class<T> clazz) {
        JAXBContext jaxbContext = createJaxbContext(clazz);
        Unmarshaller unmarshaller = getUnmarshaller(jaxbContext);
        return unmarshalFromFile( file, unmarshaller);
    }

    private static Unmarshaller getUnmarshaller(JAXBContext jaxbContext) {
        try {
            return jaxbContext.createUnmarshaller();
        } catch (JAXBException e) {
            throw new RuntimeException("Failed to created jaxb unmarshaller ", e);
        }
    }

    private static <T> T unmarshalFromFile(File file, Unmarshaller unmarshaller) {
        try {
            return (T)unmarshaller.unmarshal(file);
        } catch (Exception e) {
            throw new RuntimeException("Failed to unmarshal from file: "+file.getAbsolutePath(), e);
        }
    }

    public static void  marshal(Object jaxbElement, File file) {
        JAXBContext jaxbContext = createJaxbContext(jaxbElement.getClass());
        Marshaller marshaller = getMarshaller(jaxbContext);
        marshalToFile(jaxbElement, file, marshaller);
    }

    private static void marshalToFile(Object jaxbElement, File file, Marshaller marshaller) {
        try {
            marshaller.marshal(jaxbElement,file);
        } catch (JAXBException e) {
            throw new RuntimeException("Failed to marshal to file: "+file.getAbsolutePath(), e);
        }
    }

    private static Marshaller getMarshaller(JAXBContext jaxbContext) {
        Marshaller marshaller = null;
        try {
            marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        } catch (JAXBException e) {
            throw new RuntimeException("Failed to created jaxb marshaller ", e);
        }
        return marshaller;
    }

    private static JAXBContext createJaxbContext(Class jaxbElementClass) {
        JAXBContext jaxbContext = null;
        try {
            jaxbContext = JAXBContext.newInstance(jaxbElementClass,HashSet.class,HashMap.class );

        } catch (JAXBException e) {
            throw new RuntimeException("Failed to created jaxbContext ", e);
        }
        ;
        return jaxbContext;
    }
}
