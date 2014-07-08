package org.biouno.drmaa_pbs;

import java.util.Map;

import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobInfo;

public class JobInfoImpl implements JobInfo {

	private String jobId;
	private Map<?, ?> resourceUsage;
	private boolean exited;
	private int exitStatus;
	private String terminatingSignal;
	private boolean coreDump;
	private boolean aborted;
	private boolean signaled;
	
	JobInfoImpl() {
		super();
	}
	
	JobInfoImpl(String jobId, Map<?, ?> resourceUsage, boolean exited,
			int exitStatus, String terminatingSignal, boolean coreDump,
			boolean aborted, boolean signaled) {
		super();
		this.jobId = jobId;
		this.resourceUsage = resourceUsage;
		this.exited = exited;
		this.exitStatus = exitStatus;
		this.terminatingSignal = terminatingSignal;
		this.coreDump = coreDump;
		this.aborted = aborted;
		this.signaled = signaled;
	}



	public String getJobId() throws DrmaaException {
		return this.jobId;
	}

	@SuppressWarnings("rawtypes")
	public Map getResourceUsage() throws DrmaaException {
		return this.resourceUsage;
	}

	public boolean hasExited() throws DrmaaException {
		return this.exited;
	}

	public int getExitStatus() throws DrmaaException {
		return this.exitStatus;
	}

	public boolean hasSignaled() throws DrmaaException {
		return this.signaled;
	}

	public String getTerminatingSignal() throws DrmaaException {
		return this.terminatingSignal;
	}

	public boolean hasCoreDump() throws DrmaaException {
		return this.coreDump;
	}

	public boolean wasAborted() throws DrmaaException {
		return this.aborted;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "JobInfoImpl [jobId=" + jobId + ", resourceUsage="
				+ resourceUsage + ", exited=" + exited + ", exitStatus="
				+ exitStatus + ", terminatingSignal=" + terminatingSignal
				+ ", coreDump=" + coreDump + ", aborted=" + aborted
				+ ", signaled=" + signaled + "]";
	}
	
}
