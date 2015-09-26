/*
 * The MIT License
 *
 * Copyright (c) 2012-2015 Bruno P. Kinoshita, BioUno
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.biouno.drmaa_pbs.model;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

/**
 * A PBS job. This job can be submitted to the PBS cluster, but its state is always detached. The information in a Job
 * object is updated as you call service methods.
 *
 * @author Bruno P. Kinoshita - http://www.kinoshita.eti.br
 * @since 0.1
 */
@XStreamAlias("Job")
public class Job implements Serializable {

    private static final long serialVersionUID = 3688638797366941406L;

    /**
     * Number used to index a job in a queue (used by qnodes).
     */
    private int queueIndex;

    /**
     * Job ID. Usually a sequential identification number followed by a . (dot) and the computer name (eg:
     * 23434.thunder.mackenzie.br).
     */
    @XStreamAlias("Job_Id")
    private String id;

    /**
     * Job name. Assigned by the user.
     */
    @XStreamAlias("Job_Name")
    private String name;

    /**
     * Job owner.
     */
    @XStreamAlias("Job_Owner")
    private String owner;

    /**
     * Resources used by the job.
     */
    @XStreamAlias("resources_used")
    private Map<String, String> resourcesUsed;

    /**
     * State of the job.
     */
    @XStreamAlias("job_state")
    private String state;

    /**
     * Job queue name.
     */
    @XStreamAlias("queue")
    private String queue;

    /**
     * PBS server name.
     */
    @XStreamAlias("server")
    private String server;

    @XStreamAlias("Checkpoint")
    private String checkpoint;

    @XStreamAlias("ctime")
    private String ctime;

    @XStreamAlias("Error_Path")
    private String errorPath;

    @XStreamAlias("exec_host")
    private String execHost;

    @XStreamAlias("exec_port")
    private int execPort;

    @XStreamAlias("Hold_Types")
    private String holdTypes;

    @XStreamAlias("Join_Path")
    private String joinPath;

    @XStreamAlias("Keep_Files")
    private String keepFiles;

    @XStreamAlias("Mail_Points")
    private String mailPoints;

    @XStreamAlias("Mail_Users")
    private String mailUsers;

    @XStreamAlias("mtime")
    private String mtime;

    /**
     * Job output path.
     */
    @XStreamAlias("Output_Path")
    private String outputPath;

    /**
     * Job priority.
     */
    @XStreamAlias("Priority")
    private int priority;

    @XStreamAlias("qtime")
    private String qtime;

    /**
     * Whether a job can be rescheduled.
     */
    @XStreamAlias("Rerunable")
    private boolean rerunable;

    /**
     * List of resources used by the job.
     */
    @XStreamAlias("Resource_List")
    private Map<String, String> resourceList;

    @XStreamAlias("session_id")
    private int sessionId;

    @XStreamAlias("substate")
    private int substate;

    @XStreamAlias("Variable_List")
    private String variableList;

    @XStreamAlias("euser")
    private String euser;

    @XStreamAlias("egroup")
    private String egroup;

    @XStreamAlias("hashname")
    private String hashName;

    @XStreamAlias("queue_rank")
    private int queueRank;

    /**
     * Job queue type.
     */
    @XStreamAlias("queue_type")
    private String queueType;

    /**
     * Job comment.
     */
    @XStreamAlias("comment")
    private String comment;

    @XStreamAlias("etime")
    private String etime;

    /**
     * Job exit status.
     */
    @XStreamAlias("exit_status")
    private int exitStatus;

    /**
     * Submit args. Usually a shell script, but can include too other command line parameters.
     */
    @XStreamAlias("submit_args")
    private String submitArgs;

    /**
     * Start time.
     */
    @XStreamAlias("start_time")
    private String startTime;

    @XStreamAlias("start_count")
    private int startCount;

    private int jobArrayId;

    /**
     * Whether the job is fault-tolerant or not.
     */
    @XStreamAlias("fault_tolerant")
    private boolean faultTolerant;

    @XStreamAlias("comp_time")
    private String compTime;

    @XStreamAlias("job_radix")
    private int radix;

    /**
     * Job total runtime.
     */
    @XStreamAlias("total_runtime")
    private double totalRuntime;

    /**
     * Host that submitted the job.
     */
    @XStreamAlias("submit_host")
    private String submitHost;

    @XStreamAlias("Walltime")
    private Map<String, String> walltime;

    /**
     * Default constructor.
     */
    public Job() {
        super();

        this.resourceList = new HashMap<String, String>();
        this.resourcesUsed = new HashMap<String, String>();
        this.walltime = new HashMap<String, String>();
    }

    /**
     * @return the queueIndex
     */
    public int getQueueIndex() {
        return queueIndex;
    }

    /**
     * @param queueIndex the queueIndex to set
     */
    public void setQueueIndex(int queueIndex) {
        this.queueIndex = queueIndex;
    }

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the owner
     */
    public String getOwner() {
        return owner;
    }

    /**
     * @param owner the owner to set
     */
    public void setOwner(String owner) {
        this.owner = owner;
    }

    /**
     * @return the resourcesUsed
     */
    public Map<String, String> getResourcesUsed() {
        return resourcesUsed;
    }

    /**
     * @param resourcesUsed the resourcesUsed to set
     */
    public void setResourcesUsed(Map<String, String> resourcesUsed) {
        this.resourcesUsed = resourcesUsed;
    }

    /**
     * @return the walltime
     */
    public Map<String, String> getWalltime() {
        return walltime;
    }

    /**
     * @param walltime the walltime to set
     */
    public void setWalltime(Map<String, String> walltime) {
        this.walltime = walltime;
    }

    /**
     * @return the state
     */
    public String getState() {
        return state;
    }

    /**
     * @param state the state to set
     */
    public void setState(String state) {
        this.state = state;
    }

    /**
     * @return the queue
     */
    public String getQueue() {
        return queue;
    }

    /**
     * @param queue the queue to set
     */
    public void setQueue(String queue) {
        this.queue = queue;
    }

    /**
     * @return the server
     */
    public String getServer() {
        return server;
    }

    /**
     * @param server the server to set
     */
    public void setServer(String server) {
        this.server = server;
    }

    /**
     * @return the checkpoint
     */
    public String getCheckpoint() {
        return checkpoint;
    }

    /**
     * @param checkpoint the checkpoint to set
     */
    public void setCheckpoint(String checkpoint) {
        this.checkpoint = checkpoint;
    }

    /**
     * @return the ctime
     */
    public String getCtime() {
        return ctime;
    }

    /**
     * @param ctime the ctime to set
     */
    public void setCtime(String ctime) {
        this.ctime = ctime;
    }

    /**
     * @return the errorPath
     */
    public String getErrorPath() {
        return errorPath;
    }

    /**
     * @param errorPath the errorPath to set
     */
    public void setErrorPath(String errorPath) {
        this.errorPath = errorPath;
    }

    /**
     * @return the execHost
     */
    public String getExecHost() {
        return execHost;
    }

    /**
     * @param execHost the execHost to set
     */
    public void setExecHost(String execHost) {
        this.execHost = execHost;
    }

    /**
     * @return the execPort
     */
    public int getExecPort() {
        return execPort;
    }

    /**
     * @param execPort the execPort to set
     */
    public void setExecPort(int execPort) {
        this.execPort = execPort;
    }

    /**
     * @return the holdTypes
     */
    public String getHoldTypes() {
        return holdTypes;
    }

    /**
     * @param holdTypes the holdTypes to set
     */
    public void setHoldTypes(String holdTypes) {
        this.holdTypes = holdTypes;
    }

    /**
     * @return the joinPath
     */
    public String getJoinPath() {
        return joinPath;
    }

    /**
     * @param joinPath the joinPath to set
     */
    public void setJoinPath(String joinPath) {
        this.joinPath = joinPath;
    }

    /**
     * @return the keepFiles
     */
    public String getKeepFiles() {
        return keepFiles;
    }

    /**
     * @param keepFiles the keepFiles to set
     */
    public void setKeepFiles(String keepFiles) {
        this.keepFiles = keepFiles;
    }

    /**
     * @return the mailPoints
     */
    public String getMailPoints() {
        return mailPoints;
    }

    /**
     * @param mailPoints the mailPoints to set
     */
    public void setMailPoints(String mailPoints) {
        this.mailPoints = mailPoints;
    }

    /**
     * @return the mailUsers
     */
    public String getMailUsers() {
        return mailUsers;
    }

    /**
     * @param mailUsers the mailUsers to set
     */
    public void setMailUsers(String mailUsers) {
        this.mailUsers = mailUsers;
    }

    /**
     * @return the mtime
     */
    public String getMtime() {
        return mtime;
    }

    /**
     * @param mtime the mtime to set
     */
    public void setMtime(String mtime) {
        this.mtime = mtime;
    }

    /**
     * @return the outputPath
     */
    public String getOutputPath() {
        return outputPath;
    }

    /**
     * @param outputPath the outputPath to set
     */
    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    /**
     * @return the priority
     */
    public int getPriority() {
        return priority;
    }

    /**
     * @param priority the priority to set
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }

    /**
     * @return the qtime
     */
    public String getQtime() {
        return qtime;
    }

    /**
     * @param qtime the qtime to set
     */
    public void setQtime(String qtime) {
        this.qtime = qtime;
    }

    /**
     * @return the rerunable
     */
    public boolean isRerunable() {
        return rerunable;
    }

    /**
     * @param rerunable the rerunable to set
     */
    public void setRerunable(boolean rerunable) {
        this.rerunable = rerunable;
    }

    /**
     * @return the resourceList
     */
    public Map<String, String> getResourceList() {
        return resourceList;
    }

    /**
     * @param resourceList the resourceList to set
     */
    public void setResourceList(Map<String, String> resourceList) {
        this.resourceList = resourceList;
    }

    /**
     * @return the sessionId
     */
    public int getSessionId() {
        return sessionId;
    }

    /**
     * @param sessionId the sessionId to set
     */
    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    /**
     * @return the substate
     */
    public int getSubstate() {
        return substate;
    }

    /**
     * @param substate the substate to set
     */
    public void setSubstate(int substate) {
        this.substate = substate;
    }

    /**
     * @return the variableList
     */
    public String getVariableList() {
        return variableList;
    }

    /**
     * @param variableList the variableList to set
     */
    public void setVariableList(String variableList) {
        this.variableList = variableList;
    }

    /**
     * @return the euser
     */
    public String getEuser() {
        return euser;
    }

    /**
     * @param euser the euser to set
     */
    public void setEuser(String euser) {
        this.euser = euser;
    }

    /**
     * @return the egroup
     */
    public String getEgroup() {
        return egroup;
    }

    /**
     * @param egroup the egroup to set
     */
    public void setEgroup(String egroup) {
        this.egroup = egroup;
    }

    /**
     * @return the hashName
     */
    public String getHashName() {
        return hashName;
    }

    /**
     * @param hashName the hashName to set
     */
    public void setHashName(String hashName) {
        this.hashName = hashName;
    }

    /**
     * @return the queueRank
     */
    public int getQueueRank() {
        return queueRank;
    }

    /**
     * @param queueRank the queueRank to set
     */
    public void setQueueRank(int queueRank) {
        this.queueRank = queueRank;
    }

    /**
     * @return the queueType
     */
    public String getQueueType() {
        return queueType;
    }

    /**
     * @param queueType the queueType to set
     */
    public void setQueueType(String queueType) {
        this.queueType = queueType;
    }

    /**
     * @return the comment
     */
    public String getComment() {
        return comment;
    }

    /**
     * @param comment the comment to set
     */
    public void setComment(String comment) {
        this.comment = comment;
    }

    /**
     * @return the etime
     */
    public String getEtime() {
        return etime;
    }

    /**
     * @param etime the etime to set
     */
    public void setEtime(String etime) {
        this.etime = etime;
    }

    /**
     * @return the exitStatus
     */
    public int getExitStatus() {
        return exitStatus;
    }

    /**
     * @param exitStatus the exitStatus to set
     */
    public void setExitStatus(int exitStatus) {
        this.exitStatus = exitStatus;
    }

    /**
     * @return the submitArgs
     */
    public String getSubmitArgs() {
        return submitArgs;
    }

    /**
     * @param submitArgs the submitArgs to set
     */
    public void setSubmitArgs(String submitArgs) {
        this.submitArgs = submitArgs;
    }

    /**
     * @return the startTime
     */
    public String getStartTime() {
        return startTime;
    }

    /**
     * @param startTime the startTime to set
     */
    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    /**
     * @return the startCount
     */
    public int getStartCount() {
        return startCount;
    }

    /**
     * @param startCount the startCount to set
     */
    public void setStartCount(int startCount) {
        this.startCount = startCount;
    }

    /**
     * @return the faultTolerant
     */
    public boolean isFaultTolerant() {
        return faultTolerant;
    }

    /**
     * @param faultTolerant the faultTolerant to set
     */
    public void setFaultTolerant(boolean faultTolerant) {
        this.faultTolerant = faultTolerant;
    }

    /**
     * @return the compTime
     */
    public String getCompTime() {
        return compTime;
    }

    /**
     * @param compTime the compTime to set
     */
    public void setCompTime(String compTime) {
        this.compTime = compTime;
    }

    /**
     * @return the radix
     */
    public int getRadix() {
        return radix;
    }

    /**
     * @param radix the radix to set
     */
    public void setRadix(int radix) {
        this.radix = radix;
    }

    /**
     * @return the totalRuntime
     */
    public double getTotalRuntime() {
        return totalRuntime;
    }

    /**
     * @param totalRuntime the totalRuntime to set
     */
    public void setTotalRuntime(double totalRuntime) {
        this.totalRuntime = totalRuntime;
    }

    /**
     * @return the submitHost
     */
    public String getSubmitHost() {
        return submitHost;
    }

    /**
     * @param submitHost the submitHost to set
     */
    public void setSubmitHost(String submitHost) {
        this.submitHost = submitHost;
    }

    /**
     * @return the jobArrayId
     */
    public int getJobArrayId() {
        return jobArrayId;
    }

    /**
     * @param jobArrayId the jobArrayId to set
     */
    public void setJobArrayId(int jobArrayId) {
        this.jobArrayId = jobArrayId;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    public static XmlFile getXmlFile() {
        return new XmlFile(XSTREAM);
    }

    /**
     * Instance of XStream used to read/write a Job.
     */
    private static final XStream XSTREAM = new XStream();

    /**
     * Prepares the XStream to understand how to read/write a Job.
     */
    static {
        XSTREAM.alias("Data", List.class);
        XSTREAM.alias("Job", Job.class);
        XSTREAM.registerConverter(new MapEntryConverter());
        XSTREAM.processAnnotations(Job.class);
    }

    /**
     * Converter for entries in the PBS output that contain many children, and can be treated as a map. For example, the
     * list of resources utilised by the job.
     *
     * @author Bruno P. Kinoshita
     * @since 0.3
     */
    static class MapEntryConverter implements Converter {

        /*
         * (non-Javadoc)
         * @see com.thoughtworks.xstream.converters.ConverterMatcher#canConvert(java.lang.Class)
         */
        @Override
        public boolean canConvert(@SuppressWarnings("rawtypes") Class clazz) {
            return HashMap.class.isAssignableFrom(clazz);
        }

        /*
         * (non-Javadoc)
         * @see com.thoughtworks.xstream.converters.Converter#marshal(java.lang.Object,
         * com.thoughtworks.xstream.io.HierarchicalStreamWriter, com.thoughtworks.xstream.converters.MarshallingContext)
         */
        @Override
        public void marshal(Object value, HierarchicalStreamWriter writer, MarshallingContext context) {

            AbstractMap<?, ?> map = (AbstractMap<?, ?>) value;
            for (Object obj : map.entrySet()) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) obj;
                writer.startNode(entry.getKey().toString());
                Object val = entry.getValue();
                if (null != val) {
                    writer.setValue(val.toString());
                }
                writer.endNode();
            }

        }

        /*
         * (non-Javadoc)
         * @see
         * com.thoughtworks.xstream.converters.Converter#unmarshal(com.thoughtworks.xstream.io.HierarchicalStreamReader,
         * com.thoughtworks.xstream.converters.UnmarshallingContext)
         */
        @Override
        public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {

            Map<String, String> map = new HashMap<String, String>();

            if (reader.getNodeName().equals("Resource_List") || reader.getNodeName().equals("Walltime")
                    || reader.getNodeName().equals("resources_used")) {
                while (reader.hasMoreChildren()) {
                    reader.moveDown();

                    String key = reader.getNodeName(); // nodeName aka element's name
                    String value = reader.getValue();
                    map.put(key, value);

                    reader.moveUp();
                }
            }

            return map;
        }

    }

}
