drmaa-pbs
=========

DRMAA PBS Java library, using command executions like qsub and qstat

*This library is not complete, and implements DRMAA v1 by calling native commands (qsub, qstat, pbsnodes, etc).*

## Example

```
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
            // http://biouno.org/2014/06/21/java-drmaa-api-part-1/
            Map<String, String> options = new HashMap<String, String>();
            options.put("os", "UNIX");
            options.put("address", "localhost");
            options.put("port", "10022");
            options.put("username", "testuser");
            options.put("password", "testuser");
            options.put("connectionType", "SCP");
            ((SessionImpl) s).init("ssh", options);
            JobTemplate jt = new JobTemplateImpl();
            jt.setRemoteCommand("/home/testuser/torque.submit");
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
```

The output will be similar to.

```
Sep 26, 2015 1:26:16 PM org.biouno.drmaa_pbs.SessionImpl runJob
INFO: jobId: 9.localhost

32
Your job has been submitted with id 9.localhost
OK!
```

### Changelog
* v0.3 - Implemented basic functions for running, stopping and monitoring jobs. Tested on NCI's Raijin which uses a modified version of Pbs Pro (thanks to @kevyin)
