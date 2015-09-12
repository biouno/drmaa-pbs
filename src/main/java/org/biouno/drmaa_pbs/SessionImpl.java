package org.biouno.drmaa_pbs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
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
 * @author Kevin Ying
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
		LOCAL("local"),
		SSH("ssh"),
		SCP("scp"),
		SU("su");

		String type = "local";

		ConnectionType(String type) {
			this.type = StringUtils.defaultIfBlank(type, "local");
		}

		public String getType() {
			return type;
		}
	};

	private final ConnectionOptions connectionOptions = new ConnectionOptions();

	/* --- Constants --- */
	private static final String COMMAND_QSUB = "qsub";
	private static final String COMMAND_QSTAT = "qstat";
    private static final String COMMAND_QDEL = "qdel";

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
		this.contact = StringUtils.defaultIfBlank(contact, ConnectionType.LOCAL.getType());
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

    private String ifNotBlank(String test, String ifso) {
        return StringUtils.isNotBlank(test) ? ifso : "";
    }

    private void addIf(CmdLine cmd, Boolean test, List<String> args) {
        if (test) {
            for (String arg : args) {
                cmd.addArgument(arg);
            }
        }
    }


	/**
	 * Runs a job, by calling qsub using the job template settings, and connection info provided when the session was
	 * created.
	 * @param jt job template
	 */
	public String runJob(JobTemplate jt) throws DrmaaException {
		CmdLine cmd = CmdLine.build(COMMAND_QSUB);
        addIf(cmd, StringUtils.isNotBlank(jt.getJobName()), Arrays.asList("-N", jt.getJobName()));
//        addIf(cmd, StringUtils.isNotBlank(jt.getWorkingDirectory()), Arrays.asList("-N", jt.getWorkingDirectory()));
        addIf(cmd, StringUtils.isNotBlank(jt.getOutputPath()), Arrays.asList("-o", jt.getOutputPath()));
        addIf(cmd, StringUtils.isNotBlank(jt.getErrorPath()), Arrays.asList("-e", jt.getErrorPath()));
        addIf(cmd, jt.getJoinFiles(), Arrays.asList("-j", "oe"));
        addIf(cmd, jt.getHardWallclockTimeLimit() != 0, Arrays.asList("-l", "walltime="+jt.getHardWallclockTimeLimit()));
        LOGGER.finest("native spec " + jt.getNativeSpecification());
        addIf(cmd, StringUtils.isNotBlank(jt.getNativeSpecification()), Arrays.asList(jt.getNativeSpecification().split(" ")));


        addIf(cmd, true, Arrays.asList(jt.getRemoteCommand()));

        if (jt.getArgs() != null && jt.getArgs().size() > 0) {
        	for (Object arg : jt.getArgs()) {
        		cmd.addArgument((String) arg);
        	}
        }

        // inner class
        CommandOutput commandOutput;
		try {
			commandOutput = this.executeCommand(cmd);
		} catch (InterruptedException e) {
			throw new InvalidJobException(e.getMessage());
		}
        LOGGER.finest("qsub exit value: " + commandOutput.getExitValue());

        String out = handleCommandOutput(commandOutput);

        String jobId = out;
        LOGGER.info("jobId: " + jobId);
        return jobId.trim();
	}

    /**
     * Stops a job, by calling qdel
     * @param jobId job Id
     */
    public void stopJob(String jobId) throws DrmaaException {
        CmdLine cmd = CmdLine.build(COMMAND_QDEL, jobId);

        // inner class
        CommandOutput commandOutput;
        try {
            commandOutput = this.executeCommand(cmd);
        } catch (InterruptedException e) {
            throw new InvalidJobException(e.getMessage());
        }

        String out = handleCommandOutput(commandOutput);
        LOGGER.finest("qdel exit value: " + commandOutput.getExitValue());
    }

	@SuppressWarnings("rawtypes")
	public List runBulkJobs(JobTemplate jt, int start, int end, int incr)
			throws DrmaaException {
		// TODO Auto-generated method stub
		return null;
	}

	public void control(String jobId, int action) throws DrmaaException {
		LOGGER.log(Level.FINEST, "Control");
		// TODO implement other actions
        switch (action) {
//            case Session.SUSPEND: suspendJob(jobId); break;
//            case Session.RESUME: resumeJob(jobId); break;
//            case Session.HOLD: holdJob(jobId); break;
//            case Session.RELEASE: releaseJob(jobId); break;
            case Session.TERMINATE: stopJob(jobId); break;
            default: throw new InvalidJobException("Drmma Action not implemented yet" + action);
        }
	}

	public void synchronize(@SuppressWarnings("rawtypes") List jobIds, long timeout, boolean dispose)
			throws DrmaaException {
		LOGGER.log(Level.FINEST, "Synchronize");
		// Not implemented
	}

    private JobInfo jobToJobInfo(Job job) {
        return new JobInfoImpl(job.getId(),
                job.getResourcesUsed(),
                job.getState() == "F" || job.getState() == "X",
                job.getExitStatus(),
                "", // TODO
                false, // TODO
                false, // TODO
                false // TODO
        );
    }

	public JobInfo wait(String jobId, long timeout) throws DrmaaException {
		LOGGER.log(Level.FINEST, "wait");
        JobInfo jobInfo = jobToJobInfo(parseJobs(jobId).get(0));

        while (! jobInfo.hasExited()) {
            try {
                Thread.sleep(2000);                 //1000 milliseconds is one second.
            } catch(InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            jobInfo = jobToJobInfo(parseJobs(jobId).get(0));
        }
        return jobInfo;
	}

    private List<Job> parseJobs(String jobId) throws DrmaaException {
        CmdLine cmd = CmdLine.build(COMMAND_QSTAT, "-fx", jobId);

        // inner class
        CommandOutput commandOuptut;
        try {
            commandOuptut = this.executeCommand(cmd);
        } catch (InterruptedException e) {
            throw new InvalidJobException(e.getMessage());
        }
        LOGGER.finest("qstat exit value: " + commandOuptut.getExitValue());

        String out = handleCommandOutput(commandOuptut);

        if (commandOuptut.getExitValue() != 0) {
            throw new InvalidJobException("Failed to retrieve job program status for job id " + jobId);
        }

        List<Job> jobs;
        try {
            jobs = QSTAT_JOBS_PARSER.parse(out);
        } catch (ParseException e) {
            throw new InvalidJobException("Failed to parse qstat output: " + e.getMessage());
        }

        if (jobs.size() < 1)
            throw new InvalidJobException("Couldn't locate job " + jobId);

        return jobs;

    }

	/*
	 * (non-Javadoc)
	 * @see org.ggf.drmaa.Session#getJobProgramStatus(java.lang.String)
	 */
	public int getJobProgramStatus(String jobId) throws DrmaaException {
        Job job = parseJobs(jobId).get(0);

        /**
         * PBS Professional
         S   The job's state:

         B  Array job has at least one subjob running.

         E  Job is exiting after having run.

         F  Job is finished.

         H  Job is held.

         M  Job was moved to another server.

         Q  Job is queued.

         R  Job is running.

         S  Job is suspended.

         T  Job is being moved to new location.

         U  Cycle-harvesting job is suspended due to keyboard activity.

         W  Job is waiting for its submitter-assigned start time to be reached.

         X  Subjob has completed execution or has been deleted.

         */
        String state = job.getState();
        LOGGER.finest("state : " + state);
        switch (state) {
            case "B": return Session.RUNNING;
            case "E": return Session.RUNNING;
            case "F": return job.getExitStatus() == 0 ? Session.DONE : Session.FAILED;
            case "H": return Session.SUSPEND;
            case "M": return Session.RUNNING;
            case "Q": return Session.QUEUED_ACTIVE;
            case "R": return Session.RUNNING;
            case "S": return Session.SUSPEND;
            case "T": return Session.RUNNING;
            case "U": return Session.SUSPEND;
            case "W": return Session.QUEUED_ACTIVE;
            case "X": return job.getExitStatus() == 0 ? Session.DONE : Session.FAILED;
        }
        return Session.UNDETERMINED;
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
//            LOGGER.info("Executing " + cmd.toString()) ;
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

    private String handleCommandOutput(CommandOutput commandOutput) throws DrmaaException {

        StringBuilder out = new StringBuilder();
        String line;
        try {
            while ((line = commandOutput.getStdout().readLine()) != null) {
                out.append(line + System.getProperty("line.separator"));
            }
        } catch (IOException e) {
            throw new InvalidJobException(e.getMessage());
        } finally {
            try {
                commandOutput.getStdout().close();
            } catch (IOException e) {
                LOGGER.warning(e.getMessage());
            }
        }

        if (commandOutput.getExitValue() != 0) {
            try {
                while ((line = commandOutput.getStderr().readLine()) != null) {
                    out.append(line + System.getProperty("line.separator"));
                }
            } catch (IOException e) {
                throw new InvalidJobException(e.getMessage());
            }
            throw new InvalidJobException(
                    "\nStdout/Stderr:\n" + out.toString());
        }

        return out.toString();

    }

}
