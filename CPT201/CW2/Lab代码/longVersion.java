import java.io.File;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

public class longVersion {
    public static void main(String[] args) {
        try {
            File inputFile = new File("src/xml/XMLDoc-longVersion.xml");
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(inputFile);
            doc.getDocumentElement().normalize();

            int studentCount = countStudents(doc);
            int lowest = lowestMark(doc);
            int highest = highestMark(doc);

            System.out.println("Number of students: " + studentCount);
            System.out.println("Lowest student mark: " + lowest);
            System.out.println("Highest student mark: " + highest);

        }
         catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static int countStudents(Document doc) {
        NodeList nList = doc.getElementsByTagName("student");
        return nList.getLength();
    }

    public static int lowestMark(Document doc) {
        NodeList nList = doc.getElementsByTagName("student");
        int lowest = Integer.MAX_VALUE;

        for (int i = 0; i < nList.getLength(); i++) {
            Node node = nList.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element student = (Element) node;
                int marks = Integer.parseInt(student.getElementsByTagName("marks")
                        .item(0).getTextContent());
                if (marks < lowest) {
                    lowest = marks;
                }
            }
        }
        return lowest;
    }

    public static int highestMark(Document doc){
        NodeList nList = doc.getElementsByTagName("student");
        int highest = Integer.MIN_VALUE;

        for (int i = 0; i < nList.getLength(); i++) {
            Node node = nList.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element student = (Element) node;
                int marks = Integer.parseInt(student.getElementsByTagName("marks")
                        .item(0).getTextContent());
                if (marks > highest) {
                    highest = marks;
                }
            }
        }
        return highest;
    }



}
