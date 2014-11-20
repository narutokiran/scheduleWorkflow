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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.*;

class WorkflowLookup
{
	Class<? extends Mapper> mapperClass;
	Class<? extends Reducer> reducerClass;
	Class<? extends Reducer> combinerClass;
	Class<?> jobClass;
	Class<?> outputKeyClass;
	Class<?> outputValueClass;
	String inputDir;
	String outputDir;
	
	void setInputDir(String val)
	{
		this.inputDir=val;
	}
	String getInputDir()
	{
		return this.inputDir;
	}
	void setOutputDir(String val)
	{
		this.outputDir=val;
	}
	String getOutputDir()
	{
		return this.outputDir;
	}
	void setJobClass(String val) throws ClassNotFoundException
	{
		this.jobClass=Class.forName(val);
	}
	Class<?> getJobClass()
	{
		return this.jobClass;
	}
	void setOutputKeyClass(String val) throws ClassNotFoundException
	{
		this.outputKeyClass=Class.forName(val);
	}
	Class<?> getOutputKeyClass()
	{
		return this.outputKeyClass;
	}
	void setOutputValueClass(String val) throws ClassNotFoundException
	{
		this.outputValueClass=Class.forName(val);
	}
	Class<?> getOutputvalueClass()
	{
		return this.outputValueClass;
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void setMapperClass(String val) throws ClassNotFoundException
	{
		this.mapperClass=(Class<? extends Mapper>)Class.forName(val);
	}
	@SuppressWarnings("rawtypes")
	Class<? extends Mapper> getMapperClass()
	{
		return this.mapperClass;
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void setCombinerClass(String val) throws ClassNotFoundException
	{
		this.combinerClass=(Class<? extends Reducer>)Class.forName(val);
	}
	@SuppressWarnings("rawtypes")
	Class<? extends Reducer> getCombinerClass()
	{
		return this.combinerClass;
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void setReducerClass(String val) throws ClassNotFoundException
	{
		this.reducerClass=(Class<? extends Reducer>)Class.forName(val);
	}
	@SuppressWarnings("rawtypes")
	Class<? extends Reducer> getReducerClass()
	{
		return this.reducerClass;
	}
}

public class submitWorkFlowInitialPhase {

	@SuppressWarnings({ "deprecation", "rawtypes", "unchecked" })
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
						NodeList jobs=workflowElement.getElementsByTagName("job");
						System.out.println(workflowName+" has submitted to YARN cluster");
						for(int job=0;job<jobs.getLength();job++)
						{
							Node jobDetail= jobs.item(job);
							if(jobDetail.getNodeType() == Node.ELEMENT_NODE)
							{
								Job clientJob=new Job(new Configuration());
								Element jobElement= (Element) jobDetail;
								String jobName=new String(jobElement.getElementsByTagName("jobName").item(0).getTextContent());
								Class<?> jobClass=Class.forName(new String(jobElement.getElementsByTagName("jobClass").item(0).getTextContent()));
								clientJob.setJarByClass(jobClass);
								Class<? extends Mapper> mapperClass=(Class<? extends Mapper>)Class.forName(new String(jobElement.getElementsByTagName("mapperClass").item(0).getTextContent()));
								Class<? extends Reducer> reducerClass=(Class<? extends Reducer>)Class.forName(new String(jobElement.getElementsByTagName("reducerClass").item(0).getTextContent()));
								Class<? extends Reducer> combinerClass=(Class<? extends Reducer>)Class.forName(new String(jobElement.getElementsByTagName("combinerClass").item(0).getTextContent()));
								Class<?> outputKeyClass=Class.forName(new String(jobElement.getElementsByTagName("outputKeyClass").item(0).getTextContent()));
								Class<?> outputValueClass=Class.forName(new String(jobElement.getElementsByTagName("outputValueClass").item(0).getTextContent()));
								String inputDir=new String(jobElement.getElementsByTagName("inputDir").item(0).getTextContent());
								String outputDir=new String(jobElement.getElementsByTagName("outputDir").item(0).getTextContent());
								clientJob.setJobName(jobName);
								clientJob.setOutputKeyClass(outputKeyClass);
								clientJob.setOutputValueClass(outputValueClass);
								clientJob.setMapperClass(mapperClass);
								clientJob.setCombinerClass(combinerClass);
								clientJob.setReducerClass(reducerClass);
								FileInputFormat.addInputPath(clientJob, new Path(inputDir));
								FileOutputFormat.setOutputPath(clientJob, new Path(outputDir));
								clientJob.waitForCompletion(true);
							}
						}
						
					}
				}
			    } catch (Exception e) {
				e.printStackTrace();
			    }
	}
}

