package org.biouno.drmaa_pbs;

import java.util.HashMap;
import java.util.Map;

import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.ggf.drmaa.SessionFactory;

public class Test {

	public static void main(String[] args) {
		SessionFactory sf = SessionFactory.getFactory();
		Session s = sf.getSession();
		
		try {
			//http://biouno.org/2014/06/21/java-drmaa-api-part-1/
			Map<String, String> options = new HashMap<String, String>();
			options.put("os", "UNIX");
			options.put("address", "localhost");
			options.put("port", "2222");
			options.put("username", "vagrant");
			options.put("password", "vagrant");
			options.put("connectionType", "SCP");
			((SessionImpl) s).init("ssh", options);
			JobTemplate jt = new JobTemplateImpl();
			jt.setRemoteCommand("/vagrant/test.job");
			String id = s.runJob(jt);
			int i = s.getJobProgramStatus(id);
			System.out.println(i);
			s.deleteJobTemplate(jt);
			System.out.println("Your job has been submitted with id " + id);
			s.exit();
		} catch (DrmaaException de) {
			de.printStackTrace(System.err);
		}
		
		System.out.println("OK!");
		System.exit(1);
	}
	
}
