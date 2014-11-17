package scheduleWorkflow;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

public class submitWorkFlowInitialPhase {

	public static void main(String args[])throws Exception
	{
		 try {
			 
				File fXmlFile = new File("workflow.xml");
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(fXmlFile);
			 
				//optional, but recommended
				//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
				doc.getDocumentElement().normalize();
			 
				System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
			 
				NodeList workflowList = doc.getElementsByTagName("workflow");
			 
				for (int workflow = 0; workflow < workflowList.getLength(); workflow++) {
			 
					Node workflowDetail = workflowList.item(workflow);
					if (workflowDetail.getNodeType() == Node.ELEMENT_NODE) {
			 
						Element workflowElement = (Element) workflowDetail;
						String workflowName=new String(workflowElement.getElementsByTagName("workflowName").item(0).getTextContent());
						String jobsJarPath=new String(workflowElement.getElementsByTagName("jobsJar").item(0).getTextContent());
						NodeList jobs=workflowElement.getElementsByTagName("job");
						System.out.println(workflowName+" has submitted to YARN cluster");
						for(int job=0;job<jobs.getLength();job++)
						{
							Node jobDetail= jobs.item(job);
							if(jobDetail.getNodeType() == Node.ELEMENT_NODE)
							{
								Element jobElement= (Element) jobDetail;
								String jobName=new String(jobElement.getElementsByTagName("jobName").item(0).getTextContent());
								String mapperClass=new String(jobElement.getElementsByTagName("mapperClass").item(0).getTextContent());
								String reducerClass=new String(jobElement.getElementsByTagName("reducerClass").item(0).getTextContent());
								String combinerClass=new String(jobElement.getElementsByTagName("combinerClass").item(0).getTextContent());
								String outputKeyFormat=new String(jobElement.getElementsByTagName("outputKeyClass").item(0).getTextContent());
								String outputValueFormat=new String(jobElement.getElementsByTagName("outputValueClass").item(0).getTextContent());
								String inputDir=new String(jobElement.getElementsByTagName("inputDir").item(0).getTextContent());
								String outputDir=new String(jobElement.getElementsByTagName("outputDir").item(0).getTextContent());
								String inputFormatClass=new String(jobElement.getElementsByTagName("inputFormatClass").item(0).getTextContent());
								String outputFormatClass=new String(jobElement.getElementsByTagName("outputFormatClass").item(0).getTextContent());
								
							}
						}
						
					}
				}
			    } catch (Exception e) {
				e.printStackTrace();
			    }
	}
}

