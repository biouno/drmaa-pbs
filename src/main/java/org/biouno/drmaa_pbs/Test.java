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
