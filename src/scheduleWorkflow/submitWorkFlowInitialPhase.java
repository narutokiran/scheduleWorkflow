package scheduleWorkflow;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.File;
import java.io.IOException;

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
	String predecessor[]=new String[1000];
	String successor[]=new String[1000];
	String outputPath;
	String status;
	String jobName;
	int inputPathsCount,predecessorCount,successorCount;
	WorkflowLookup()
	{
		inputPathsCount=0;predecessorCount=0;successorCount=0;status="Initialize";
	}
	int getPredecessorCount()
	{
		return this.predecessorCount;
	}
	int getSuccessorCount()
	{
		return this.successorCount;
	}
	int getInputPathsCount()
	{
		return this.inputPathsCount;
	}
	void setPredecessor(String val)
	{
		this.predecessor[this.predecessorCount++]=val;
	}
	String getPredecessor(int val)
	{
		return this.predecessor[val];
	}
	String getSuccessor(int val)
	{
		return this.successor[val];
	}
	void setSuccessor(String val)
	{
		this.successor[this.successorCount++]=val;
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
	void submitHadoopMRJob(Job clientJob) throws IOException, ClassNotFoundException, InterruptedException
	{
		clientJob.setJobName(this.getJobName());
		clientJob.setJarByClass(this.getJobClass());
		clientJob.setOutputKeyClass(this.getOutputKeyClass());
		clientJob.setOutputValueClass(this.getOutputvalueClass());
		clientJob.setMapperClass(this.getMapperClass());
		clientJob.setCombinerClass(this.getCombinerClass());
		clientJob.setReducerClass(this.getReducerClass());
		String inputs="";
		for(int input=0;input<this.getInputPathsCount()-1;input++)
			inputs+=this.getInputPaths(input)+",";
		inputs+=this.getInputPaths(this.getInputPathsCount()-1);
		FileInputFormat.addInputPaths(clientJob, inputs);
		FileOutputFormat.setOutputPath(clientJob, new Path(this.getOutputPath()));
		this.setJobStatus("Submitted");
		clientJob.submit();
		System.out.println("Submitted "+this.getJobName()+" job to the YARN cluster");
	}
}

public class submitWorkFlowInitialPhase {

	private static String predecessorSet;

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
								/* Parse XML and get the requirement value to run Hadoop jobs in the YARN cluster */
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
								predecessorSet = new String(jobElement.getElementsByTagName("predecessor").item(0).getTextContent());
								predecessorSet=predecessorSet.trim();
								//System.out.println(predecessorSet.isEmpty());
								if(predecessorSet!=null && !predecessorSet.isEmpty())
								{
									
									String predecessors[]=predecessorSet.split(",");
									//System.out.println(predecessors[0]);
									for(int predecessor=0;predecessor<predecessors.length;predecessor++)
										jobLookUp[job].setPredecessor(predecessors[predecessor]);
								}
							}
						}
						for(int job=0;job<jobLength;job++)
						{
							for(int successor=0;successor<jobLength;successor++)
							{
								if(job!=successor)
								{
									for(int predecessor=0;predecessor<jobLookUp[successor].getPredecessorCount();predecessor++)
									{
										if((jobLookUp[successor].getPredecessor(predecessor)).equals(jobLookUp[job].getJobName()))
										{
											jobLookUp[job].setSuccessor(jobLookUp[successor].getJobName());
											break;
										}
									}
								}
							}
						}
						
						/*Find root jobs */
						String rootJobs[]=new String[jobLength+2];
						int rootJobCount=0;
						for(int job=0;job<jobLength;job++)
						{
							//System.out.println(jobLookUp[job].getPredecessorCount());
							if(jobLookUp[job].getPredecessorCount()==0)
							{
								rootJobs[rootJobCount++]=jobLookUp[job].getJobName();
								//System.out.println(jobLookUp[job].getJobName());
							}
						}
						
						/*Submitting root jobs to YARN cluster and adding successor jobs to the WaitQueue */
						ArrayList<String> waitQueue=new ArrayList<String>();
						for(int rootJob=0;rootJob<rootJobCount;rootJob++)
						{
							int jobIndex=-1;
							for(int job=0;job<jobLength;job++)
							{
								if(jobLookUp[job].getJobName().equals(rootJobs[rootJob]))
								{
									jobIndex=job;
									//System.out.println(jobIndex);
									break;
								}
							}
							if(jobIndex!=-1){
								/*Add successors to the wait Queue */
								for(int successor=0;successor<jobLookUp[jobIndex].getSuccessorCount();successor++)
								{
									if(waitQueue.indexOf(jobLookUp[jobIndex].getSuccessor(successor))==-1)
									{
										waitQueue.add(jobLookUp[jobIndex].getSuccessor(successor));
									}
								}
								/*Submit a job to YARN cluster */
								jobLookUp[jobIndex].submitHadoopMRJob(clientJob[jobIndex]);
							}
						}
						/*Submitting pending jobs to YARN cluster */
						while(waitQueue.size()!=0)
						{
							for(int waitJob=0;waitJob<waitQueue.size();waitJob++)
							{
								/*flag variable to check all predecessor jobs are completed */
								int flag=0;
								int jobIndex=-1;
								/*finding job index of the current pending job */
								for(int job=0;job<jobLength;job++)
								{
									if(jobLookUp[job].getJobName().equals(waitQueue.get(waitJob)))
									{
										jobIndex=job;
										break;
									}
								}
								if(jobIndex!=-1)
								{
									for(int predecessor=0;predecessor<jobLookUp[jobIndex].getPredecessorCount();predecessor++)
									{
										//System.out.println(predecessor+"-"+jobLookUp[jobIndex].getPredecessor(predecessor));
										int predecessorIndex=-1;
										/*find the predecessor job index of the current pending job */
										for(int job=0;job<jobLength;job++)
										{
											if(jobLookUp[job].getJobName().equals(jobLookUp[jobIndex].getPredecessor(predecessor)))
											{
												predecessorIndex=job;
												//System.out.println(jobIndex+"-"+predecessorIndex+"-"+jobLookUp[jobIndex].getPredecessorCount());
												break;
											}
										}
										if(predecessorIndex!=-1)
										{
											/*check if all predecessors are submitted and completed */
											if(!jobLookUp[predecessorIndex].getJobStatus().equals("Initialize") && clientJob[predecessorIndex].isComplete())
											{
												//System.out.println(predecessorIndex+" is completed");
												jobLookUp[predecessorIndex].setJobStatus("Completed");
											}
											else
											{
												flag=1;
												break;
											}
										}
									}
								}
								/*if all predecessors are submitted and completed the submit the current pending job to YARN cluster and remove it from the waitQueue */
								if(flag==0)
								{
									for(int successor=0;successor<jobLookUp[jobIndex].getSuccessorCount();successor++)
									{
										if(waitQueue.indexOf(jobLookUp[jobIndex].getSuccessor(successor))==-1)
										{
											waitQueue.add(jobLookUp[jobIndex].getSuccessor(successor));
										}
									}
									waitQueue.remove(jobLookUp[jobIndex].getJobName());
									jobLookUp[jobIndex].submitHadoopMRJob(clientJob[jobIndex]);
								}
							}
						}		
					}
				}
			    } catch (Exception e) {
				e.printStackTrace();
			    }
	}
}

