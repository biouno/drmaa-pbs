package org.biouno.drmaa_pbs;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.FileTransferMode;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.PartialTimestamp;

public class JobTemplateImpl implements JobTemplate {

	private String remoteCommand;
	private List<?> args;
	private int state;
	private Map<?, ?> env;
	private String wd;
	private String category;
	private String spec;
	private Set<?> email;
	private boolean blockEmail;
	private PartialTimestamp startTime;
	private String name;
	private String inputPath;
	private String outputPath;
	private String errorPath;
	private boolean join;
	private FileTransferMode mode;
	private PartialTimestamp deadline;
	private long hardWallclockLimit;
	private long softWallclockLimit;
	private long hardRunLimit;
	private long softRunLimit;

	public void setRemoteCommand(String remoteCommand) throws DrmaaException {
		this.remoteCommand = remoteCommand;
	}

	public String getRemoteCommand() throws DrmaaException {
		return this.remoteCommand;
	}

	@SuppressWarnings("rawtypes")
	public void setArgs(List args) throws DrmaaException {
		this.args = args;
	}

	@SuppressWarnings("rawtypes")
	public List getArgs() throws DrmaaException {
		return args;
	}

	public void setJobSubmissionState(int state) throws DrmaaException {
		this.state = state;
	}

	public int getJobSubmissionState() throws DrmaaException {
		return this.state;
	}

	@SuppressWarnings("rawtypes")
	public void setJobEnvironment(Map env) throws DrmaaException {
		this.env = env;
	}

	@SuppressWarnings("rawtypes")
	public Map getJobEnvironment() throws DrmaaException {
		return this.env;
	}

	public void setWorkingDirectory(String wd) throws DrmaaException {
		this.wd = wd;
	}

	public String getWorkingDirectory() throws DrmaaException {
		return this.wd;
	}

	public void setJobCategory(String category) throws DrmaaException {
		this.category = category;
	}

	public String getJobCategory() throws DrmaaException {
		return this.category;
	}

	public void setNativeSpecification(String spec) throws DrmaaException {
		this.spec = spec;
	}

	public String getNativeSpecification() throws DrmaaException {
		return this.spec;
	}

	@SuppressWarnings("rawtypes")
	public void setEmail(Set email) throws DrmaaException {
		this.email = email;
	}

	@SuppressWarnings("rawtypes")
	public Set getEmail() throws DrmaaException {
		return this.email;
	}

	public void setBlockEmail(boolean blockEmail) throws DrmaaException {
		this.blockEmail = blockEmail;
	}

	public boolean getBlockEmail() throws DrmaaException {
		return this.blockEmail;
	}

	public void setStartTime(PartialTimestamp startTime) throws DrmaaException {
		this.startTime = startTime;
	}

	public PartialTimestamp getStartTime() throws DrmaaException {
		return this.startTime;
	}

	public void setJobName(String name) throws DrmaaException {
		this.name = name;
	}

	public String getJobName() throws DrmaaException {
		return this.name;
	}

	public void setInputPath(String inputPath) throws DrmaaException {
		this.inputPath = inputPath;
	}

	public String getInputPath() throws DrmaaException {
		return this.inputPath;
	}

	public void setOutputPath(String outputPath) throws DrmaaException {
		this.outputPath = outputPath;
	}

	public String getOutputPath() throws DrmaaException {
		return this.outputPath;
	}

	public void setErrorPath(String errorPath) throws DrmaaException {
		this.errorPath = errorPath;
	}

	public String getErrorPath() throws DrmaaException {
		return this.errorPath;
	}

	public void setJoinFiles(boolean join) throws DrmaaException {
		this.join = join;
	}

	public boolean getJoinFiles() throws DrmaaException {
		return this.join;
	}

	public void setTransferFiles(FileTransferMode mode) throws DrmaaException {
		this.mode = mode;
	}

	public FileTransferMode getTransferFiles() throws DrmaaException {
		return this.mode;
	}

	public void setDeadlineTime(PartialTimestamp deadline) throws DrmaaException {
		this.deadline = deadline;
	}

	public PartialTimestamp getDeadlineTime() throws DrmaaException {
		return this.deadline;
	}

	public void setHardWallclockTimeLimit(long hardWallclockLimit) throws DrmaaException {
		this.hardWallclockLimit = hardWallclockLimit;
	}

	public long getHardWallclockTimeLimit() throws DrmaaException {
		return this.hardWallclockLimit;
	}

	public void setSoftWallclockTimeLimit(long softWallclockLimit) throws DrmaaException {
		this.softWallclockLimit = softWallclockLimit;
	}

	public long getSoftWallclockTimeLimit() throws DrmaaException {
		return this.softWallclockLimit;
	}

	public void setHardRunDurationLimit(long hardRunLimit) throws DrmaaException {
		this.hardRunLimit = hardRunLimit;
	}

	public long getHardRunDurationLimit() throws DrmaaException {
		return this.hardRunLimit;
	}

	public void setSoftRunDurationLimit(long softRunLimit) throws DrmaaException {
		this.softRunLimit = softRunLimit;
	}

	public long getSoftRunDurationLimit() throws DrmaaException {
		return this.softRunLimit;
	}

	@SuppressWarnings("rawtypes")
	public Set getAttributeNames() throws DrmaaException {
		return null;
	}
	
}
