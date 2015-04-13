package scheduleWorkflow;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.*;
import java.util.ArrayList;
public class submitWorkFlowInitialPhase {
	public static void main(String args[])throws Exception
	{
		 try {
			 
				File fXmlFile = new File("workflowExample.xml");
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
								System.out.println("Root Job "+jobLookUp[jobIndex].getJobName());
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
											if(jobLookUp[predecessorIndex].isJobComplete() && !jobLookUp[predecessorIndex].getJobStatus().equals("Initialize")){
												System.out.println(jobLookUp[predecessorIndex].getJobName()+" is completed and its status was "+jobLookUp[predecessorIndex].getJobStatus());	
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
					    	if(!jobLookUp[job].isJobComplete())
					    		job--;
					    }
					    System.out.println("All jobs in the workflow - "+workflowName + " had completed");
					}
				}
			    } catch (Exception e) {
				e.printStackTrace();
			    }
			    	}
}

