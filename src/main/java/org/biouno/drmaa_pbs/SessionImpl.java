package org.biouno.drmaa_pbs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang.StringUtils;
import org.biouno.drmaa_pbs.model.Job;
import org.biouno.drmaa_pbs.parser.ParseException;
import org.biouno.drmaa_pbs.parser.QstatJobsParser;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.InvalidJobException;
import org.ggf.drmaa.JobInfo;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.Version;

import com.xebialabs.overthere.CmdLine;
import com.xebialabs.overthere.ConnectionOptions;
import com.xebialabs.overthere.Overthere;
import com.xebialabs.overthere.OverthereConnection;
import com.xebialabs.overthere.OverthereProcess;

/**
 * PBS DRMAA Session implementation.
 * @author Bruno P. Kinoshita
 */
public class SessionImpl implements Session {
	
	private static final Logger LOGGER = Logger.getLogger(SessionImpl.class.getName());

	private static final QstatJobsParser QSTAT_JOBS_PARSER = new QstatJobsParser();
	
	/**
	 * Session connection type.
	 */
	private String contact;
	
	/**
	 * Utility enum with the connection types
	 * @author kinow
	 *
	 */
	public enum ConnectionType {
		LOCAL("LOCAL"),
		SSH("SSH"),
		SCP("SCP"),
		SU("SU");
		
		String type = "local";
		
		ConnectionType(String type) {
			this.type = StringUtils.defaultIfBlank(type, "LOCAL");
		}
		
		public String getType() {
			return type;
		}
	};
	
	private final ConnectionOptions connectionOptions = new ConnectionOptions();
	
	/* --- Constants --- */
	private static final String COMMAND_QSUB = "qsub";
	private static final String COMMAND_QSTAT = "qstat";
	
	/**
	 * Hidden constructor. Package only.
	 */
	/* package */ SessionImpl() {
		LOGGER.log(Level.FINEST, "New session created!");
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.ggf.drmaa.Session#init(java.lang.String)
	 */
	public void init(String contact) throws DrmaaException {
		LOGGER.log(Level.FINEST, "Session init()");
		this.contact = StringUtils.defaultIfBlank(contact, "local");
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * <p>The extra options parameter is used to fill the connection options for overthere library. It is
	 * used to establish connections with SSH, SFTP or LOCAL (Process) servers.</p>
	 * 
	 * @param contact connection type
	 * @param options connection options
	 */
	public void init(String contact, Map<String, String> options) {
		LOGGER.log(Level.FINEST, "Session init() with connection options");
		this.contact = StringUtils.defaultIfBlank(contact, ConnectionType.LOCAL.getType());
		if (options != null && options.size() > 0) {
			for (Entry<String, String> entry : options.entrySet()) {
				connectionOptions.set(entry.getKey(), entry.getValue());
			}
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.ggf.drmaa.Session#exit()
	 */
	public void exit() throws DrmaaException {
		LOGGER.log(Level.FINEST, "Session exit()");
		// Not implemented
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.ggf.drmaa.Session#createJobTemplate()
	 */
	public JobTemplate createJobTemplate() throws DrmaaException {
		LOGGER.log(Level.FINEST, "Creating new job template");
		return new JobTemplateImpl();
	}

	/*
	 * (non-Javadoc)
	 * @see org.ggf.drmaa.Session#deleteJobTemplate(org.ggf.drmaa.JobTemplate)
	 */
	public void deleteJobTemplate(JobTemplate jt) throws DrmaaException {
		LOGGER.log(Level.FINEST, "Delete job template");
		// Not implemented
	}

	/**
	 * Runs a job, by calling qsub using the job template settings, and connection info provided when the session was 
	 * created.
	 * @param jt job template
	 */
	public String runJob(JobTemplate jt) throws DrmaaException {
		CmdLine cmd = CmdLine.build(COMMAND_QSUB, jt.getRemoteCommand());
        if (jt.getArgs() != null && jt.getArgs().size() > 0) {
        	for (Object arg : jt.getArgs()) {
        		cmd.addArgument((String) arg);
        	}
        }
        
        // inner class
        CommandOutput commandOuptut;
		try {
			commandOuptut = this.executeCommand(cmd);
		} catch (InterruptedException e) {
			throw new InvalidJobException(e.getMessage());
		}
        LOGGER.info("qsub exit value: " + commandOuptut.getExitValue());
        StringBuilder out = new StringBuilder();
        String line;
        try {
			while ((line = commandOuptut.getStdout().readLine()) != null) {
				out.append(line + System.getProperty("line.separator"));
			}
		} catch (IOException e) {
			throw new InvalidJobException(e.getMessage());
		} finally {
			try {
				commandOuptut.getStdout().close();
			} catch (IOException e) {
				LOGGER.warning(e.getMessage());
			}
		}
        
        if (commandOuptut.getExitValue() != 0) {
        	StringBuilder err = new StringBuilder();
            try {
				while ((line = commandOuptut.getStderr().readLine()) != null) {
					out.append(line + System.getProperty("line.separator"));
				}
			} catch (IOException e) {
				throw new InvalidJobException(e.getMessage());
			}
        	throw new InvalidJobException("Failed to submit job " + jt.getJobName() + ". Error output: " + err.toString());
        }
        
        String jobId = out.toString();
        return jobId.trim();
	}

	@SuppressWarnings("rawtypes")
	public List runBulkJobs(JobTemplate jt, int start, int end, int incr)
			throws DrmaaException {
		// TODO Auto-generated method stub
		return null;
	}

	public void control(String jobId, int action) throws DrmaaException {
		LOGGER.log(Level.FINEST, "Control");
		// Not implemented
	}

	public void synchronize(@SuppressWarnings("rawtypes") List jobIds, long timeout, boolean dispose)
			throws DrmaaException {
		LOGGER.log(Level.FINEST, "Synchronize");
		// Not implemented
	}

	public JobInfo wait(String jobId, long timeout) throws DrmaaException {
		LOGGER.log(Level.FINEST, "wait");
		// Not implemented
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.ggf.drmaa.Session#getJobProgramStatus(java.lang.String)
	 */
	public int getJobProgramStatus(String jobId) throws DrmaaException {
		CmdLine cmd = CmdLine.build(COMMAND_QSTAT, "-f", jobId);
        
        // inner class
        CommandOutput commandOuptut;
		try {
			commandOuptut = this.executeCommand(cmd);
		} catch (InterruptedException e) {
			throw new InvalidJobException(e.getMessage());
		}
        LOGGER.info("qsub exit value: " + commandOuptut.getExitValue());
        StringBuilder out = new StringBuilder();
        String line;
        try {
			while ((line = commandOuptut.getStdout().readLine()) != null) {
				out.append(line + System.getProperty("line.separator"));
			}
		} catch (IOException e) {
			throw new InvalidJobException(e.getMessage());
		} finally {
			try {
				commandOuptut.getStdout().close();
			} catch (IOException e) {
				LOGGER.warning(e.getMessage());
			}
		}
        
        if (commandOuptut.getExitValue() != 0) {
        	StringBuilder err = new StringBuilder();
            try {
				while ((line = commandOuptut.getStderr().readLine()) != null) {
					out.append(line + System.getProperty("line.separator"));
				}
			} catch (IOException e) {
				throw new InvalidJobException(e.getMessage());
			}
        	throw new InvalidJobException("Failed to retrieve job program status for job id " + jobId + ". Error output: " + err.toString());
        }
        
        List<Job> jobs;
		try {
			jobs = QSTAT_JOBS_PARSER.parse(out.toString());
		} catch (ParseException e) {
			throw new InvalidJobException("Failed to parse qstat output: " + e.getMessage());
		}
        
        if (jobs.size() < 1)
        	throw new InvalidJobException("Couldn't locate job " + jobId);
        
        Job job = jobs.get(0);
        System.out.println(job.getState());
        
        return -1;
	}

	/*
	 * (non-Javadoc)
	 * @see org.ggf.drmaa.Session#getContact()
	 */
	public String getContact() {
		return contact;
	}

	/*
	 * (non-Javadoc)
	 * @see org.ggf.drmaa.Session#getVersion()
	 */
	public Version getVersion() {
		return new Version(0, 1);
	}

	/*
	 * (non-Javadoc)
	 * @see org.ggf.drmaa.Session#getDrmSystem()
	 */
	public String getDrmSystem() {
		return "PBS/Torque";
	}

	/*
	 * (non-Javadoc)
	 * @see org.ggf.drmaa.Session#getDrmaaImplementation()
	 */
	public String getDrmaaImplementation() {
		return getDrmSystem();
	}
	
	/*
     * ------------------------------
     * Utility methods
     * ------------------------------
     */
	public CommandOutput executeCommand(CmdLine cmd) throws InterruptedException {
		final OverthereConnection connection = Overthere.getConnection(this.getContact(), this.connectionOptions);
        
        OverthereProcess process = null;
        BufferedReader stdout = null;
        BufferedReader stderr = null;
        int exitValue = -1;
        try {
        	LOGGER.info("Executing " + cmd.toString());
            process = connection.startProcess(cmd);
            stdout = new BufferedReader(new InputStreamReader(process.getStdout()));
            stderr = new BufferedReader(new InputStreamReader(process.getStderr()));
            
            if (process != null) {
            	try {
    				exitValue = process.waitFor();
    			} catch (InterruptedException e) {
    				LOGGER.severe(e.getMessage());
    				throw e;
    			}
            }
        } finally {
        	connection.close();
        }
        
        return new CommandOutput(exitValue, stdout, stderr);
	}
	
	private class CommandOutput {

		private final int exitValue;
		private final BufferedReader stdout;
		private final BufferedReader stderr;

		public CommandOutput(int exitValue, BufferedReader stdout,
				BufferedReader stderr) {
			super();
			this.exitValue = exitValue;
			this.stdout = stdout;
			this.stderr = stderr;
		}

		/**
		 * @return the exitValue
		 */
		public int getExitValue() {
			return exitValue;
		}

		/**
		 * @return the stdout
		 */
		public BufferedReader getStdout() {
			return stdout;
		}

		/**
		 * @return the stderr
		 */
		public BufferedReader getStderr() {
			return stderr;
		}

	}

}
