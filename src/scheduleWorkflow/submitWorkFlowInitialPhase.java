package scheduleWorkflow;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.*;
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

import java.net.URI;
import java.net.URISyntaxException;
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
	String jobType;
	String seq2SparseWeight;
	String testOutput;
	String trainingOutput;
	String randomSelectionPct;
	String xm;
	String labelIndexPath;
	String MAHOUT_HOME;
	String modelPath;
	 URI url;
	boolean afterJobCompletion;
	Job clientJob;
	Process process;
	int inputPathsCount,predecessorCount,successorCount;
	@SuppressWarnings("deprecation")
	WorkflowLookup()throws IOException, URISyntaxException
	{
		process=null;
		url=new URI("hdfs://localhost:9000");
		afterJobCompletion=true;
		MAHOUT_HOME=System.getenv("MAHOUT_HOME");
		inputPathsCount=0;predecessorCount=0;successorCount=0;status="Initialize";clientJob=new Job(new Configuration());
	}
	void setModelPath(String val)
	{
		this.modelPath=val;
	}
	String getModelPath()
	{
		return this.modelPath;
	}
	void setLabelIndexPath(String val)
	{
		this.labelIndexPath=val;
	}
	String getLabelIndexPath()
	{
		return this.labelIndexPath;
	}
	void setTestOutput(String val)
	{
		this.testOutput=val;
	}
	String getTestOutput()
	{
		return this.testOutput;
	}
	void setTrainingOutput(String val)
	{
		this.trainingOutput=val;
	}
	String getTrainingOutput()
	{
		return this.trainingOutput;
	}
	void setRandomSelectionPct(String val)
	{
		this.randomSelectionPct=val;
	}
	String getRandomSelectionPct()
	{
		return this.randomSelectionPct;
	}
	void setXm(String val)
	{
		this.xm=val;
	}
	String getXm()
	{
		return this.xm;
	}
	void setseq2SparseWeight(String val)
	{
		this.seq2SparseWeight=val;
	}
	String getSeq2SparseWeight()
	{
		return this.seq2SparseWeight;
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
	void setJobType(String val)
	{
		this.jobType=val;
	}
	String getJobType()
	{
		return this.jobType;
	}
	boolean isRunning(Process process)throws Exception {
    try {
       int exitValue=process.exitValue();
       if(exitValue!=0)
       {
       	System.out.println("Abnormal Termination");
       	BufferedReader br=new BufferedReader(new InputStreamReader(process.getErrorStream()));
       	String s;
       	while((s=br.readLine())!=null)
       		System.out.println(s);
       }
        return false;
    } catch (Exception e) {
        return true;
    }
	}
	void submitJob() throws IOException, ClassNotFoundException, InterruptedException
	{

		if(this.jobType.equals("MahoutSeqDumper"))
		{
			this.setJobStatus("Submitted");
			String filePathComponent[]=this.getOutputPath().split("/");
			String fileOutputPath=this.MAHOUT_HOME+"/"+filePathComponent[filePathComponent.length-1];
			//single input path - default it will be stroed in 0th index
			this.process = Runtime.getRuntime().exec(this.MAHOUT_HOME+"/bin/mahout seqdumper -i "+this.getInputPaths(0)+" -o "+fileOutputPath);
			System.out.println("Submitted "+this.getJobName()+" job to the YARN cluster");
		}
		else if(this.jobType.equals("MahoutTestNB"))
		{
			this.setJobStatus("Submitted");
			//single input path - default it will be stroed in 0th index
			this.process = Runtime.getRuntime().exec(this.MAHOUT_HOME+"/bin/mahout testnb -i "+this.getInputPaths(0)+" -o "+this.getOutputPath()+" -l "+this.getLabelIndexPath()+" -m "+this.getModelPath()+" -ow -c");
			System.out.println("Submitted "+this.getJobName()+" job to the YARN cluster");
		}
		else if(this.jobType.equals("MahoutTrainNB"))
		{
			this.setJobStatus("Submitted");
			//single input path - default it will be stroed in 0th index
			this.process = Runtime.getRuntime().exec(this.MAHOUT_HOME+"/bin/mahout trainnb -i "+this.getInputPaths(0)+" -el -o "+this.getOutputPath()+" -li "+this.getLabelIndexPath()+" -ow -c");
			System.out.println("Submitted "+this.getJobName()+" job to the YARN cluster");
		}
		else if(this.jobType.equals("MahoutSplit"))
		{
			this.setJobStatus("Submitted");
			//single input path - default it will be stroed in 0th index
			this.process = Runtime.getRuntime().exec(this.MAHOUT_HOME+"/bin/mahout split -i "+this.getInputPaths(0)+" --trainingOutput "+this.getTrainingOutput()+" --testOutput "+this.getTestOutput()+" --randomSelectionPct "+this.getRandomSelectionPct()+" --overwrite --sequenceFiles -xm "+this.getXm());
			System.out.println("Submitted "+this.getJobName()+" job to the YARN cluster");
		}
		else if(this.jobType.equals("MahoutSeq2Sparse"))
		{
			this.setJobStatus("Submitted");
			//single input path - default it will be stroed in 0th index
			this.process = Runtime.getRuntime().exec(this.MAHOUT_HOME+"/bin/mahout seq2sparse -i "+this.getInputPaths(0)+" -o "+this.getOutputPath()+" -lnorm -nv -wt "+this.getSeq2SparseWeight());
			System.out.println("Submitted "+this.getJobName()+" job to the YARN cluster");
		}
		else if(this.jobType.equals("MahoutSeqDirectory"))
		{
			this.setJobStatus("Submitted");
			//single input path - default it will be stroed in 0th index
			this.process = Runtime.getRuntime().exec(this.MAHOUT_HOME+"/bin/mahout seqdirectory -i "+this.getInputPaths(0)+" -o "+this.getOutputPath()+" -ow");
			System.out.println("Submitted "+this.getJobName()+" job to the YARN cluster");
		}
		else if(this.jobType.equals("HadoopMR"))
		{
			this.clientJob.setJobName(this.getJobName());
			this.clientJob.setJarByClass(this.getJobClass());
			this.clientJob.setOutputKeyClass(this.getOutputKeyClass());
			this.clientJob.setOutputValueClass(this.getOutputvalueClass());
			this.clientJob.setMapperClass(this.getMapperClass());
			this.clientJob.setCombinerClass(this.getCombinerClass());
			this.clientJob.setReducerClass(this.getReducerClass());
			String inputs="";
			for(int input=0;input<this.getInputPathsCount()-1;input++)
				inputs+=this.getInputPaths(input)+",";
			inputs+=this.getInputPaths(this.getInputPathsCount()-1);
			FileInputFormat.addInputPaths(this.clientJob, inputs);
			FileOutputFormat.setOutputPath(this.clientJob, new Path(this.getOutputPath()));
			this.setJobStatus("Submitted");
			this.clientJob.submit();
			System.out.println("Submitted "+this.getJobName()+" job to the YARN cluster");
		}
	}
	void parseXML(Node jobDetail) throws ClassNotFoundException, DOMException
	{
		if(jobDetail.getNodeType() == Node.ELEMENT_NODE && this.jobType.equals("MahoutSeqDumper"))
		{
			Element jobElement=(Element) jobDetail;
			this.setJobName(new String(jobElement.getElementsByTagName("jobName").item(0).getTextContent()));
			// Single input path - value will be stored in 0th index.
			this.setInputPaths(new String(jobElement.getElementsByTagName("input").item(0).getTextContent()));	
			String predecessorSet = new String(jobElement.getElementsByTagName("predecessor").item(0).getTextContent());
			predecessorSet=predecessorSet.trim();
			if(predecessorSet!=null && !predecessorSet.isEmpty())
			{
				String predecessors[]=predecessorSet.split(",");
				for(int predecessor=0;predecessor<predecessors.length;predecessor++)
					this.setPredecessor(predecessors[predecessor]);
			}
			this.setOutputPath(new String(jobElement.getElementsByTagName("output").item(0).getTextContent()));
		}
		else if(jobDetail.getNodeType() == Node.ELEMENT_NODE && this.jobType.equals("MahoutTestNB"))
		{
			Element jobElement=(Element) jobDetail;
			this.setJobName(new String(jobElement.getElementsByTagName("jobName").item(0).getTextContent()));
			// Single input path - value will be stored in 0th index.
			this.setInputPaths(new String(jobElement.getElementsByTagName("input").item(0).getTextContent()));	
			String predecessorSet = new String(jobElement.getElementsByTagName("predecessor").item(0).getTextContent());
			predecessorSet=predecessorSet.trim();
			if(predecessorSet!=null && !predecessorSet.isEmpty())
			{
				String predecessors[]=predecessorSet.split(",");
				for(int predecessor=0;predecessor<predecessors.length;predecessor++)
					this.setPredecessor(predecessors[predecessor]);
			}
			this.setOutputPath(new String(jobElement.getElementsByTagName("output").item(0).getTextContent()));
			this.setLabelIndexPath(new String(jobElement.getElementsByTagName("labelIndex").item(0).getTextContent()));
			this.setModelPath(new String(jobElement.getElementsByTagName("model").item(0).getTextContent()));
		}
		else if(jobDetail.getNodeType() == Node.ELEMENT_NODE && this.jobType.equals("MahoutTrainNB"))
		{
			Element jobElement=(Element) jobDetail;
			this.setJobName(new String(jobElement.getElementsByTagName("jobName").item(0).getTextContent()));
			// Single input path - value will be stored in 0th index.
			this.setInputPaths(new String(jobElement.getElementsByTagName("input").item(0).getTextContent()));	
			String predecessorSet = new String(jobElement.getElementsByTagName("predecessor").item(0).getTextContent());
			predecessorSet=predecessorSet.trim();
			if(predecessorSet!=null && !predecessorSet.isEmpty())
			{
				String predecessors[]=predecessorSet.split(",");
				for(int predecessor=0;predecessor<predecessors.length;predecessor++)
					this.setPredecessor(predecessors[predecessor]);
			}
			this.setOutputPath(new String(jobElement.getElementsByTagName("output").item(0).getTextContent()));
			this.setLabelIndexPath(new String(jobElement.getElementsByTagName("labelIndex").item(0).getTextContent()));
		}
		else if(jobDetail.getNodeType() == Node.ELEMENT_NODE && this.jobType.equals("MahoutSplit"))
		{
			Element jobElement=(Element) jobDetail;
			this.setJobName(new String(jobElement.getElementsByTagName("jobName").item(0).getTextContent()));
			// Single input path - value will be stored in 0th index.
			this.setInputPaths(new String(jobElement.getElementsByTagName("input").item(0).getTextContent()));	
			String predecessorSet = new String(jobElement.getElementsByTagName("predecessor").item(0).getTextContent());
			predecessorSet=predecessorSet.trim();
			if(predecessorSet!=null && !predecessorSet.isEmpty())
			{
				String predecessors[]=predecessorSet.split(",");
				for(int predecessor=0;predecessor<predecessors.length;predecessor++)
					this.setPredecessor(predecessors[predecessor]);
			}
			this.setTrainingOutput(new String(jobElement.getElementsByTagName("trainingOutput").item(0).getTextContent()));
			this.setTestOutput(new String(jobElement.getElementsByTagName("testOutput").item(0).getTextContent()));
			this.setRandomSelectionPct(new String(jobElement.getElementsByTagName("randomSelectionPct").item(0).getTextContent()));
			this.setXm(new String(jobElement.getElementsByTagName("xm").item(0).getTextContent()));
		}
		else if(jobDetail.getNodeType() == Node.ELEMENT_NODE && this.jobType.equals("MahoutSeq2Sparse"))
		{
			Element jobElement=(Element) jobDetail;
			this.setJobName(new String(jobElement.getElementsByTagName("jobName").item(0).getTextContent()));
			// Single input path - value will be stored in 0th index.
			this.setInputPaths(new String(jobElement.getElementsByTagName("input").item(0).getTextContent()));
			this.setOutputPath(new String(jobElement.getElementsByTagName("output").item(0).getTextContent()));
			String predecessorSet = new String(jobElement.getElementsByTagName("predecessor").item(0).getTextContent());
			predecessorSet=predecessorSet.trim();
			if(predecessorSet!=null && !predecessorSet.isEmpty())
			{
				String predecessors[]=predecessorSet.split(",");
				for(int predecessor=0;predecessor<predecessors.length;predecessor++)
					this.setPredecessor(predecessors[predecessor]);
			}
			this.setseq2SparseWeight(new String(jobElement.getElementsByTagName("weight").item(0).getTextContent()));
		}
		else if(jobDetail.getNodeType() == Node.ELEMENT_NODE && this.jobType.equals("MahoutSeqDirectory"))
		{
			Element jobElement=(Element) jobDetail;
			this.setJobName(new String(jobElement.getElementsByTagName("jobName").item(0).getTextContent()));
			// Single input path - value will be stored in 0th index.
			this.setInputPaths(new String(jobElement.getElementsByTagName("input").item(0).getTextContent()));
			this.setOutputPath(new String(jobElement.getElementsByTagName("output").item(0).getTextContent()));
			String predecessorSet = new String(jobElement.getElementsByTagName("predecessor").item(0).getTextContent());
			predecessorSet=predecessorSet.trim();
			if(predecessorSet!=null && !predecessorSet.isEmpty())
			{
				String predecessors[]=predecessorSet.split(",");
				for(int predecessor=0;predecessor<predecessors.length;predecessor++)
					this.setPredecessor(predecessors[predecessor]);
			}
		}
		else if(jobDetail.getNodeType() == Node.ELEMENT_NODE && this.jobType.equals("HadoopMR"))
		{
			/* Parse XML and get the requirement value to run Hadoop jobs in the YARN cluster */
			Element jobElement= (Element) jobDetail;
			this.setJobName(new String(jobElement.getElementsByTagName("jobName").item(0).getTextContent()));
			this.setJobClass(new String(jobElement.getElementsByTagName("jobClass").item(0).getTextContent()));	
			this.setMapperClass(new String(jobElement.getElementsByTagName("mapperClass").item(0).getTextContent()));
			this.setReducerClass(new String(jobElement.getElementsByTagName("reducerClass").item(0).getTextContent()));
			this.setCombinerClass(new String(jobElement.getElementsByTagName("combinerClass").item(0).getTextContent()));
			this.setOutputKeyClass(new String(jobElement.getElementsByTagName("outputKeyClass").item(0).getTextContent()));
			this.setOutputValueClass(new String(jobElement.getElementsByTagName("outputValueClass").item(0).getTextContent()));
			NodeList inputPaths=jobElement.getElementsByTagName("inputPaths");
			Node inputPathsDetail=inputPaths.item(0);
			Element inputPathsElement=(Element) inputPathsDetail;
			NodeList inputPath=inputPathsElement.getElementsByTagName("input");
			for(int input=0;input<inputPath.getLength();input++)
			{
				Node inputPathDetail=inputPath.item(input);
				Element inputPathElement=(Element) inputPathDetail;
				this.setInputPaths(new String(inputPathElement.getElementsByTagName("path").item(0).getTextContent()));
			}
			this.setOutputPath(new String(jobElement.getElementsByTagName("outputPath").item(0).getTextContent()));
			String predecessorSet = new String(jobElement.getElementsByTagName("predecessor").item(0).getTextContent());
			predecessorSet=predecessorSet.trim();
			if(predecessorSet!=null && !predecessorSet.isEmpty())
			{
				String predecessors[]=predecessorSet.split(",");
				for(int predecessor=0;predecessor<predecessors.length;predecessor++)
					this.setPredecessor(predecessors[predecessor]);
			}
		}
	}
	boolean isJobComplete() throws Exception
	{
		if(this.jobType.equals("MahoutSeqDumper") && !this.getJobStatus().equals("Initialize") && this.isRunning(this.process))
			{
				return false;
			}
		else if(this.jobType.equals("MahoutTestNB") && !this.getJobStatus().equals("Initialize") && this.isRunning(this.process)){
				return false;
		}
		else if(this.jobType.equals("MahoutTrainNB") && !this.getJobStatus().equals("Initialize") && this.isRunning(this.process))
			{
				return false;
			}
		else if(this.jobType.equals("MahoutSplit") && !this.getJobStatus().equals("Initialize") && this.isRunning(this.process))
			{
				return false;
			}
		else if(this.jobType.equals("MahoutSeq2Sparse") && !this.getJobStatus().equals("Initialize") && this.isRunning(this.process))
			{
				return false;
			}
		else if(this.jobType.equals("MahoutSeqDirectory") && !this.getJobStatus().equals("Initialize") && this.isRunning(this.process))
			{
				return false;
			}
		else if(this.jobType.equals("HadoopMR") && !this.getJobStatus().equals("Initialize") && !this.clientJob.isSuccessful())
			{
				return false;
			}
		return true;
	}
	void afterJob() throws Exception
	{
		if(this.afterJobCompletion)
		{
			if(this.jobType.equals("MahoutSeqDumper"))
			{
				String filePathComponent[]=this.getOutputPath().split("/");
				String localFilePath=this.MAHOUT_HOME+"/"+filePathComponent[filePathComponent.length-1];
				FileSystem hdfs =FileSystem.get(this.url,new Configuration());
				Path hdfsFilePath= new Path(this.getOutputPath());
				hdfs.copyFromLocalFile(new Path(localFilePath),hdfsFilePath);
				File file=new File(localFilePath);
				if(!file.delete())
					System.out.println("Problem in deleting a temporary file");
				hdfs.close();
				//Set afterJobCompletion false so it will not call again when we check for predecessors.
				this.afterJobCompletion=false;
			}
		}
	}
}

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
			 
				NodeList workflowList = doc.getElementsByTagName("workflow");
			 
				for (int workflow = 0; workflow < workflowList.getLength(); workflow++) {
			 
					Node workflowDetail = workflowList.item(workflow);
					if (workflowDetail.getNodeType() == Node.ELEMENT_NODE) {
			 
						Element workflowElement = (Element) workflowDetail;
						String workflowName=new String(workflowElement.getElementsByTagName("workflowName").item(0).getTextContent());
						NodeList jobs=workflowElement.getElementsByTagName("job");
						System.out.println("Workflow - "+workflowName+" has submitted to YARN cluster");
						int jobLength=jobs.getLength();
						WorkflowLookup jobLookUp[]=new WorkflowLookup[jobLength+2];
						for(int job=0;job<jobLength;job++)
						{
							jobLookUp[job]=new WorkflowLookup();
							Node jobDetail= jobs.item(job);
							String jobType=jobDetail.getAttributes().getNamedItem("type").getNodeValue();
							jobLookUp[job].setJobType(jobType);
							jobLookUp[job].parseXML(jobDetail);
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
							if(jobLookUp[job].getPredecessorCount()==0)
							{
								rootJobs[rootJobCount++]=jobLookUp[job].getJobName();
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
								jobLookUp[jobIndex].submitJob();
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
										int predecessorIndex=-1;
										/*find the predecessor job index of the current pending job */
										for(int job=0;job<jobLength;job++)
										{
											if(jobLookUp[job].getJobName().equals(jobLookUp[jobIndex].getPredecessor(predecessor)))
											{
												predecessorIndex=job;
												break;
											}
										}
										if(predecessorIndex!=-1)
										{
											//System.out.println(jobLookUp[predecessorIndex].getJobName());
											if(jobLookUp[predecessorIndex].isJobComplete()){
												jobLookUp[predecessorIndex].setJobStatus("Completed");
												jobLookUp[predecessorIndex].afterJob();
											}
											/*check if all predecessors are submitted and completed */
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
									jobLookUp[jobIndex].submitJob();
								}
							}
						}
						//if all jobs are not completed, then wait for those
					    for(int job=0;job<jobLength;job++)
					    {
					    	if(jobLookUp[job].process!=null)
					    		jobLookUp[job].process.waitFor();
					    	jobLookUp[job].afterJob(); //Any work has to be done (copy, move files) after a job has completed.
					    }
					    System.out.println("All jobs in the workflow - "+workflowName + " had completed");
					}
				}
			    } catch (Exception e) {
				e.printStackTrace();
			    }
			    	}
}

