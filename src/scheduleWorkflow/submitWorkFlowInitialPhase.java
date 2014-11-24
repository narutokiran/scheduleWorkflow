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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

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
	String inputPaths[]=new String[100];
	String outputPath;
	String status;
	String jobName;
	int inputPathsCount;
	WorkflowLookup()
	{
		inputPathsCount=0;
	}
	int getInputPathsCount()
	{
		return this.inputPathsCount;
	}
	void setJobName(String val)
	{
		this.jobName=val;
	}
	String getJobName()
	{
		return this.jobName;
	}
	void setInputPaths(String val)
	{
		this.inputPaths[inputPathsCount++]=val;
	}
	String getInputPaths(int val)
	{
		return this.inputPaths[val];
	}
	void setOutputPath(String val)
	{
		this.outputPath=val;
	}
	String getOutputPath()
	{
		return this.outputPath;
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
	void setJobStatus(String val)
	{
		this.status=val;
	}
	String getJobStatus()
	{
		return this.status;
	}
}

public class submitWorkFlowInitialPhase {

	@SuppressWarnings({ "deprecation"})
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
			 
				NodeList workflowList = doc.getElementsByTagName("workflow");
			 
				for (int workflow = 0; workflow < workflowList.getLength(); workflow++) {
			 
					Node workflowDetail = workflowList.item(workflow);
					if (workflowDetail.getNodeType() == Node.ELEMENT_NODE) {
			 
						Element workflowElement = (Element) workflowDetail;
						String workflowName=new String(workflowElement.getElementsByTagName("workflowName").item(0).getTextContent());
						NodeList jobs=workflowElement.getElementsByTagName("job");
						System.out.println(workflowName+" has submitted to YARN cluster");
						int jobLength=jobs.getLength();
						WorkflowLookup jobLookUp[]=new WorkflowLookup[jobLength+2];
						Job clientJob[]=new Job[jobLength+2];
						for(int job=0;job<jobLength;job++)
						{
							jobLookUp[job]=new WorkflowLookup();
							clientJob[job]=new Job(new Configuration());
							Node jobDetail= jobs.item(job);
							if(jobDetail.getNodeType() == Node.ELEMENT_NODE)
							{
								/* Parse XML and get the requirement value to run hadoop jobs in the YARN cluster */
								Element jobElement= (Element) jobDetail;
								jobLookUp[job].setJobName(new String(jobElement.getElementsByTagName("jobName").item(0).getTextContent()));
								jobLookUp[job].setJobClass(new String(jobElement.getElementsByTagName("jobClass").item(0).getTextContent()));	
								jobLookUp[job].setMapperClass(new String(jobElement.getElementsByTagName("mapperClass").item(0).getTextContent()));
								jobLookUp[job].setReducerClass(new String(jobElement.getElementsByTagName("reducerClass").item(0).getTextContent()));
								jobLookUp[job].setCombinerClass(new String(jobElement.getElementsByTagName("combinerClass").item(0).getTextContent()));
								jobLookUp[job].setOutputKeyClass(new String(jobElement.getElementsByTagName("outputKeyClass").item(0).getTextContent()));
								jobLookUp[job].setOutputValueClass(new String(jobElement.getElementsByTagName("outputValueClass").item(0).getTextContent()));
								NodeList inputPaths=jobElement.getElementsByTagName("inputPaths");
								Node inputPathsDetail=inputPaths.item(0);
								Element inputPathsElement=(Element) inputPathsDetail;
								NodeList inputPath=inputPathsElement.getElementsByTagName("input");
								for(int input=0;input<inputPath.getLength();input++)
								{
									Node inputPathDetail=inputPath.item(input);
									Element inputPathElement=(Element) inputPathDetail;
									jobLookUp[job].setInputPaths(new String(inputPathElement.getElementsByTagName("path").item(0).getTextContent()));
								}
									jobLookUp[job].setOutputPath(new String(jobElement.getElementsByTagName("outputPath").item(0).getTextContent()));
								
								/*Submitting the job to YARN Cluster*/
								clientJob[job].setJobName(jobLookUp[job].getJobName());
								clientJob[job].setJarByClass(jobLookUp[job].getJobClass());
								clientJob[job].setOutputKeyClass(jobLookUp[job].getOutputKeyClass());
								clientJob[job].setOutputValueClass(jobLookUp[job].getOutputvalueClass());
								clientJob[job].setMapperClass(jobLookUp[job].getMapperClass());
								clientJob[job].setCombinerClass(jobLookUp[job].getCombinerClass());
								clientJob[job].setReducerClass(jobLookUp[job].getReducerClass());
								String inputs="";
								for(int input=0;input<jobLookUp[job].getInputPathsCount()-1;input++)
									inputs+=jobLookUp[job].getInputPaths(input)+",";
								inputs+=jobLookUp[job].getInputPaths(jobLookUp[job].getInputPathsCount()-1);
								FileInputFormat.addInputPaths(clientJob[job], inputs);
								FileOutputFormat.setOutputPath(clientJob[job], new Path(jobLookUp[job].getOutputPath()));
								jobLookUp[job].setJobStatus("Submitted");
								clientJob[job].submit();
								System.out.println("hi");
							}
						}
						
					}
				}
			    } catch (Exception e) {
				e.printStackTrace();
			    }
	}
}

