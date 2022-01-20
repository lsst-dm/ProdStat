#!/usr/bin/env python
# This file is part of ProdStat package.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import glob
from yaml import load, FullLoader
from datetime import datetime
from pytz import timezone
from GetButlerStat import *
from GetPanDaStat import *
from JiraUtils import *
import subprocess


class DRPUtils:
    def __init__(self):
        """
        The class to organize various DRP utilities in a single class
        """
        self.ju = JiraUtils()
        self.ajira, self.user_name = self.ju.get_login()

    @staticmethod
    def parse_template(bps_yaml_file):
        """
        :param bps_yaml_file:
        :return: bpsstr
        :return: kwd
        """
        kwlist = ["campaign", "project", "payload", "pipelineYaml"]
        kw = {
            "payload": [
                "payloadName",
                "butlerConfig",
                "dataQuery",
                "inCollection",
                "sw_image",
                "output",
            ]
        }
        f = open(bps_yaml_file)
        d = load(f, Loader=FullLoader)
        f.close()

        kwd = dict()
        bpsstr = "BPS Submit Keywords:\n{code}\n"
        for k, v in d.items():
            if k in kwlist:
                if k in kw:
                    for k1 in kw[k]:
                        kwd[k1] = v[k1]
                        bpsstr += str(k1) + ":" + str(v[k1]) + "\n"
                else:
                    kwd[k] = v
                    bpsstr += str(k) + ": " + str(v) + "\n"

        for kw in kwd:
            v = kwd[kw]
            bpsstr = bpsstr.replace("{" + str(kw) + "}", v)
        return bpsstr, kwd

    @staticmethod
    def parse_yaml(bps_yaml_file, ts):
        """

        :param bps_yaml_file:
        :param ts:
        :return:
        """
        kwlist = ["campaign", "project", "payload", "pipelineYaml"]
        kw = {
            "payload": [
                "payloadName",
                "butlerConfig",
                "dataQuery",
                "inCollection",
                "sw_image",
                "output",
            ]
        }
        f = open(bps_yaml_file, "r")
        d = load(f, Loader=FullLoader)
        f.close()
        kwd = dict()
        bpsstr = "BPS Submit Keywords:\n{code}\n"
        for k, v in d.items():
            if k in kwlist:
                if k in kw:
                    for k1 in kw[k]:
                        kwd[k1] = v[k1]
                        bpsstr += str(k1) + ":" + str(v[k1]) + "\n"
                else:
                    kwd[k] = v
                    bpsstr += str(k) + ": " + str(v) + "\n"
        uniqid = "./" + os.path.dirname(bps_yaml_file) + "/submit/" + kwd["output"]
        for k in kwd:
            v = kwd[k]
            uniqid = uniqid.replace("{" + str(k) + "}", v)
        print(uniqid)
        if ts == "0":
            allpath = glob.glob(uniqid + "/*")
            allpath.sort()
            longpath = allpath[-1]
            ts = os.path.basename(longpath)
        else:
            ts = ts.upper()
            longpath = uniqid + "/" + ts
        # print(longpath)
        submittedyaml = kwd["output"] + "_" + ts
        for k in kwd:
            v = kwd[k]
            submittedyaml = submittedyaml.replace("{" + str(k) + "}", v)
        submittedyaml = submittedyaml.replace("/", "_")
        fullbpsyaml = longpath + "/" + submittedyaml + "_config.yaml"
        # print(fullbpsyaml)
        origyamlfile = longpath + "/" + os.path.basename(bps_yaml_file)
        bpsstr = bpsstr + "bps_submit_yaml_file: " + str(bps_yaml_file) + "\n"
        akwd = dict()
        if os.path.exists(origyamlfile):
            (
                mode,
                ino,
                dev,
                nlink,
                uid,
                gid,
                size,
                atime,
                origyamlfilemtime,
                ctime,
            ) = os.stat(origyamlfile)
            if os.path.exists(fullbpsyaml):
                print(
                    "full bps yaml file exists -- updating start graph generation timestamp"
                )
                (
                    mode,
                    ino,
                    dev,
                    nlink,
                    uid,
                    gid,
                    size,
                    atime,
                    origyamlfilemtime,
                    ctime,
                ) = os.stat(fullbpsyaml)
                # print(origyamlfile,origyamlfilemtime,time.ctime(origyamlfilemtime))
            skwlist = ["bps_defined", "executionButler", "computeSite", "cluster"]
            skw = {
                "bps_defined": ["operator", "uniqProcName"],
                "executionButler": ["queue"],
            }

            f = open(fullbpsyaml)
            d = load(f, Loader=FullLoader)
            f.close()
            print("submityaml keys:", d)
            for k, v in d.items():
                if k in skwlist:
                    if k in skw:
                        for k1 in skw[k]:
                            akwd[k1] = v[k1]
                            bpsstr += str(k1) + ":" + str(v[k1]) + "\n"
                    else:
                        akwd[k] = v
                        bpsstr += str(k) + ": " + str(v) + "\n"

            print("akwd", akwd)
            print("kwd", kwd)
            print(bpsstr)
            qgraphfile = longpath + "/" + submittedyaml + ".qgraph"
            (
                mode,
                ino,
                dev,
                nlink,
                uid,
                gid,
                qgraphfilesize,
                atime,
                mtime,
                ctime,
            ) = os.stat(qgraphfile)
            # print(qgraphfile,qgraphfilesize)
            bpsstr += (
                "qgraphsize:" + str("{:.1f}".format(qgraphfilesize / 1.0e6)) + "MB\n"
            )
            qgraphout = longpath + "/" + "quantumGraphGeneration.out"
            (
                mode,
                ino,
                dev,
                nlink,
                uid,
                gid,
                size,
                atime,
                qgraphoutmtime,
                ctime,
            ) = os.stat(qgraphout)
            f = open(qgraphout)
            qgstat = f.read()
            f.close()
            m = re.search("QuantumGraph contains (.*) quanta for (.*) task", qgstat)
            if m:
                nquanta = m.group(1)
                ntasks = m.group(2)
                bpsstr += "nTotalQuanta:" + str("{:d}".format(int(nquanta))) + "\n"
                bpsstr += "nTotalPanDATasks:" + str("{:d}".format(int(ntasks))) + "\n"

            # QuantumGraph contains 310365 quanta for 5 tasks
            # print(qgraphout,qgraphoutmtime,time.ctime(qgraphoutmtime))
            execbutlerdb = longpath + "/EXEC_REPO-" + submittedyaml + "/gen3.sqlite3"
            (
                mode,
                ino,
                dev,
                nlink,
                uid,
                gid,
                butlerdbsize,
                atime,
                butlerdbmtime,
                ctime,
            ) = os.stat(execbutlerdb)
            # print(execbutlerdb,butlerdbsize,butlerdbmtime,time.ctime(butlerdbmtime))
            bpsstr += (
                "execbutlersize:"
                + str("{:.1f}".format(butlerdbsize / 1.0e6))
                + "MB"
                + "\n"
            )
            timetomakeqg = qgraphoutmtime - origyamlfilemtime
            timetomakeexecbutlerdb = butlerdbmtime - qgraphoutmtime
            # print(timetomakeqg,timetomakeexecbutlerdb)
            bpsstr += (
                "timeConstructQGraph:"
                + str("{:.1f}".format(timetomakeqg / 60.0))
                + "min\n"
            )
            bpsstr += (
                "timeToFillExecButlerDB:"
                + str("{:.1f}".format(timetomakeexecbutlerdb / 60.0))
                + "min\n"
            )
            print(bpsstr)
        # sys.exit(1)
        return bpsstr, kwd, akwd, ts

    def drp_init(self, template, issue_name, drpi):
        """
        Creates or update DRP issue
        :param template: <bps_submit_yaml_template>. Template file with place holders for
                        start/end dataset/visit/tracts (will be attached to Production Issue)")
        :param issue_name: <Production Issue> Pre-existing issue of form PREOPS-XXX (later DRP-XXX) to update with link
                     to ProdStat tracking issue(s) -- should match issue in template keyword "
        :param drpi: DRP-issue. If present in form DRP-XXX, redo by overwriting an existing DRP-issue.
                     If not present, create a new DRP-issue.
                     All ProdStat plots and links for group of bps submits will be tracked
                     off this DRP-issue.
                     Production Issue will be updated with a link to this issue, by updating
                     description (or later by using subtask link if all are DRP type). ")
        :return:
        """
        bpsstr, kwd = self.parse_template(template)
        stepname = kwd["pipelineYaml"]
        p = re.compile("(.*)#(.*)")
        m = p.match(stepname)
        print("stepname " + stepname)
        if m:
            steppath = m.group(1)
            stepcut = m.group(2)
        else:
            steppath = ""
            stepcut = ""

        print("steplist " + stepcut)
        print("steppath " + steppath)
        bpsstr += "pipelineYamlSteps: " + stepcut + "\n{code}\n"

        uniqid = kwd["output"]
        for k in kwd:
            v = kwd[k]
            uniqid = uniqid.replace("{" + str(k) + "}", v)
        uniqid = uniqid.replace("/", "_")
        #
        if drpi == "DRP0":
            drp_issue = self.ajira.create_issue(
                project="DRP",
                issuetype="Task",
                summary="a new issue",
                description=bpsstr,
                components=[{"name": "Test"}],
            )
        else:
            drp_issue = self.ajira.issue(drpi)
        issue_dict = {"summary": stepcut + "#" + uniqid, "description": bpsstr}
        self.ju.update_issue(drp_issue, issue_dict)

        jirapissue = self.ajira.issue(issue_name)

        olddesc = jirapissue.fields.description

        p = re.compile("(.*)(Production Statistics:DRP-[0-9]*)", re.DOTALL)
        match = p.match(olddesc)
        if match:
            olddesc.replace(m.group(2), "Production Statistics:" + str(drp_issue), 1)
            desc = olddesc
        else:
            desc = olddesc + "\n Production Statistics:" + str(drp_issue) + " here\n"
        jirapissue.update(fields={"description": desc})
        print(
            "Production Issue: "
            + str(jirapissue)
            + " description updated with DRP issue link"
        )
        print("DRP issue for ProdStats : " + str(drp_issue))

    @staticmethod
    def parse_panda_table(intab):
        """

        :param intab: input txt table created by GetPanDaStat.py
        :return:
        """
        print("infile is " + intab)
        f = open(intab, "r")
        done = 0
        nquanta = dict()
        startdate = dict()
        secperstep = dict()
        sumtime = dict()
        wallhr = dict()
        maxmem = dict()
        totsumsec = 0
        totmaxmem = 0
        preqid = 0
        upn = ""
        pstat = ""
        pntasks = ""
        pnfiles = ""
        pnremain = ""
        pnproc = ""
        pnfin = ""
        pnfail = ""
        psubfin = ""
        while done == 0:
            lin = f.readline()
            if len(lin) == 0:
                done = 1
                continue
            a = lin.strip()
            print("a is" + str(a))
            # b=a.split("|")
            # b=a.split("│")
            b = a.split("│")
            l = len(b)
            print("len:" + str(l) + " a is:" + a)
            if l > 1:
                print("b1 " + b[1])
            if l > 1 and b[1].strip()[0:2] == "20":
                print("b1 " + b[1])
                upn = b[1].strip()
                pstat = b[2]
                pntasks = b[3]
                pnfiles = b[4]
                pnremain = b[5]
                pnproc = b[6]
                pnfin = b[7]
                pnfail = b[8]
                psubfin = b[9]
                print(
                    "statline:",
                    upn,
                    pstat,
                    pntasks,
                    pnfiles,
                    pnremain,
                    pnproc,
                    pnfin,
                    pnfail,
                    psubfin,
                )
                continue
            taskname = ""
            if l > 5:
                staskname = b[1].lstrip(" ").rstrip(" ")
                print("len tn:" + str(len(staskname)))
                print("staskname:" + str(staskname))
                splittask = staskname.split("_")
                if len(splittask) > 1:
                    taskname = splittask[0]
                    preqid = splittask[1]
                else:
                    taskname = ""
            print("taskname:" + str(taskname))
            print("preqid:" + str(preqid))
            if taskname != "" and taskname != "Campaign":
                nquanta[taskname] = int(b[2])
                startdate[taskname] = b[3]
                secperstep[taskname] = float(b[5])
                wallclock = b[4]
                hms = wallclock.split(":")
                daysplit = hms[0].split("day")
                print("day:", daysplit, len(daysplit))
                if len(daysplit) > 1:
                    daysplit2 = hms[0].split("days,")
                    if len(daysplit2) > 1:
                        wallhr[taskname] = (
                            int(daysplit2[0]) * 24
                            + int(daysplit2[1])
                            + float(hms[1]) / 60.0
                            + float(hms[2]) / 3600.0
                        )
                    else:
                        daysplit = hms[0].split("day,")
                        wallhr[taskname] = (
                            int(daysplit[0]) * 24
                            + int(daysplit[1])
                            + float(hms[1]) / 60.0
                            + float(hms[2]) / 3600.0
                        )
                else:
                    wallhr[taskname] = (
                        int(hms[0]) + float(hms[1]) / 60.0 + float(hms[2]) / 3600.0
                    )
                sumtime[taskname] = nquanta[taskname] * secperstep[taskname] / 3600.0
                maxmem[taskname] = float(b[7])
                totsumsec = totsumsec + sumtime[taskname]
                totmaxmem = totmaxmem + wallhr[taskname]
        result = (
            totmaxmem,
            totsumsec,
            nquanta,
            startdate,
            secperstep,
            wallhr,
            sumtime,
            maxmem,
            upn,
            preqid,
            pstat,
            pntasks,
            pnfiles,
            pnremain,
            pnproc,
            pnfin,
            pnfail,
            psubfin,
        )
        return result

    @staticmethod
    def parse_drp(steppath, tocheck):
        """
        If the DRP.yaml as put out by the Pipeline team changes
        -- this file should be updated.
        It is in  $OBS_LSST_DIR/pipelines/imsim/DRP.yaml
        :param steppath:
        :param tocheck:
        :return:
        """
        stepenvironsplit = steppath.split("}")
        if len(stepenvironsplit) > 1:
            envvar = stepenvironsplit[0][2:]
            restofpath = stepenvironsplit[1]
        else:
            envvar = ""
            restofpath = steppath
        print(envvar, restofpath)
        drpfile = open(os.environ.get(envvar) + restofpath)
        drpyaml = load(drpfile, Loader=FullLoader)
        # eventually need to load the includes for more details
        taskdict = dict()
        stepdict = dict()
        stepdesdict = dict()
        subsets = drpyaml["subsets"]
        for k, v in subsets.items():
            stepname = k
            tasklist = v["subset"]
            tasklist.insert(0, "pipetaskInit")
            tasklist.append("mergeExecutionButler")
            # print(len(tasklist))
            # print('tasklist:',tasklist)
            taskdict["pipetaskInit"] = stepname
            for t in tasklist:
                taskdict[t] = stepname
            taskdict["mergeExecutionButler"] = stepname
            stepdict[stepname] = tasklist
            stepdesdict[stepname] = v["description"]
        # assumes tasknames are unique
        # i.e. that there's not more than one step
        # with the same taskname
        # print("steps")
        # for k,v in stepdict.items():
        # print(k,v)
        # print(stepdesdict[k])
        steplist = tocheck.split(",")
        retdict = list()
        for i in steplist:
            if i in stepdict:
                for j in stepdict[i]:
                    retdict.append([i, j])
            elif i in taskdict:
                retdict.append([taskdict[i], i])
        return retdict

    @staticmethod
    def parse_butler_table(intab):
        """
        :param intab:
        :return:
        """
        print("infile is " + intab)
        f = open(intab, "r")
        done = 0
        nquanta = dict()
        startdate = dict()
        secperstep = dict()
        sumtime = dict()
        maxmem = dict()
        totsumsec = 0
        totmaxmem = 0
        upn = ""
        while done == 0:
            lin = f.readline()
            if len(lin) == 0:
                done = 1
                continue
            a = lin.strip()
            b = a.split()
            print("b:" + str(b))
            if len(b) > 0 and b[0] == "with":
                upn = b[2][2:-2]
                print("uid:" + upn)
                continue
            print("a is" + str(a))
            # b=a.split("|")
            b = a.split("│")
            lm = len(b)
            print("len:" + str(lm))
            if lm > 5:
                taskname = b[1].lstrip(" ").rstrip(" ")
                print("len tn:" + str(len(taskname)))
                print("taskname:" + str(taskname))
                if taskname != "":
                    nquanta[taskname] = int(b[2])
                    startdate[taskname] = b[3]
                    secperstep[taskname] = float(b[4])
                    sumtime[taskname] = (
                        nquanta[taskname] * secperstep[taskname] / 3600.0
                    )
                    maxmem[taskname] = float(b[6])
                    totsumsec = totsumsec + sumtime[taskname]
                    if maxmem[taskname] > totmaxmem and taskname != "Campaign":
                        totmaxmem = maxmem[taskname]
                        print("bumping maxmem to " + str(totmaxmem))
        result = (
            totmaxmem,
            totsumsec,
            nquanta,
            startdate,
            secperstep,
            sumtime,
            maxmem,
            upn,
        )
        return result

    def drp_stat_update(self, pissue, drpi):
        """

        :param pissue:
        :param drpi:
        :return:
        """
        #        ts = "0"
        # get summary from DRP ticket
        in_pars = dict()
        drp_issue = self.ajira.issue(drpi)
        summary = drp_issue.fields.summary
        print("summary is", summary)
        olddesc = drp_issue.fields.description
        print("old desc is", olddesc)
        substr = "{code}"
        idx = olddesc.find(substr, olddesc.find(substr) + 1)
        print(idx)
        newdesc = olddesc[0:idx] + "{code}\n"
        print("new is", newdesc)
        pattern0 = "(.*)#(.*)(20[0-9][0-9][0-9][0-9][0-9][0-9][Tt][0-9][0-9][0-9][0-9][0-9][0-9][Zz])"
        mts = re.match(pattern0, summary)
        if mts:
            what = mts.group(1)
            ts = mts.group(3)
        else:
            what = "0"
            ts = "0"

        print(ts, what)
        # run butler and/or panda stats for one timestamp.
        in_pars["Butler"] = "s3://butler-us-central1-panda-dev/dc2/butler-external.yaml"
        in_pars["Jira"] = str(pissue)
        in_pars["collType"] = ts.upper()
        in_pars["workNames"] = ""
        in_pars["maxtask"] = 100
        get_butler_stat = GetButlerStat(**in_pars)
        get_butler_stat.run()
        butfilename = "/tmp/butlerStat-" + str(pissue) + ".txt"
        if os.path.exists(butfilename):
            fbstat = open(butfilename, "r")
            butstat = fbstat.read()
            fbstat.close()
        else:
            butstat = "\n"
        panfilename = "/tmp/pandaStat-" + str(pissue) + ".txt"
        in_pars["collType"] = ts.lower()
        get_panda_stat = GetPanDaStat(**in_pars)
        get_panda_stat.run()
        if os.path.exists(panfilename):
            fpstat = open(panfilename, "r")
            statstr = fpstat.read()
            fpstat.close()
            fstat = open("/tmp/pandaWfStat-" + str(pissue) + ".csv", "r")
            line1 = fstat.readline()
            line2 = fstat.readline()
            a = line2.split(",")
            fstat.close()
            # print(len(a),a)
            pstat = a[1]
            pntasks = int(a[2][:-2])
            pnfiles = int(a[3][:-2])
            pnproc = int(a[4][:-2])
            pnfin = int(a[6][:-2])
            pnfail = int(a[7][:-2])
            psubfin = int(a[8][:-2])
            curstat = (
                "Status:"
                + str(pstat)
                + " nTasks:"
                + str(pntasks)
                + " nFiles:"
                + str(pnfiles)
                + " nRemain:"
                + str(pnproc)
                + " nProc:"
                + " nFinish:"
                + str(pnfin)
                + " nFail:"
                + str(pnfail)
                + " nSubFinish:"
                + str(psubfin)
                + "\n"
            )
        else:
            statstr = "\n"
            curstat = "\n"
        # sys.exit(1)
        pupn = ts
        # print('pupn:',pupn)
        year = str(pupn[0:4])
        month = str(pupn[4:6])
        # day=str(pupn[6:8])
        day = str("01")
        print("year:", year)
        print("year:", month)
        print("year:", day)
        link = (
            "https://panda-doma.cern.ch/tasks/?taskname=*"
            + pupn.lower()
            + "*&date_from="
            + str(day)
            + "-"
            + str(month)
            + "-"
            + str(year)
            + "&days=62&sortby=time-ascending"
        )
        print("link:", link)
        linkline = "PanDA link:" + link + "\n"
        # print(butstat+statstr+curstat)
        nowut = (
            datetime.datetime.now(timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S") + "Z"
        )
        issue_dict = {"description": newdesc + butstat + linkline + statstr + curstat}
        drp_issue.update(fields=issue_dict)
        print("issue:" + str(drp_issue) + " Stats updated")

    @staticmethod
    def parse_issue_desc(jdesc, jsummary):
        """
        Extracts some information from jira issue
        :param jdesc: issue description field
        :param jsummary:  issue summary field
        :return:
        """
        pattern0 = "(.*)#(.*)(20[0-9][0-9][0-9][0-9][0-9][0-9][Tt][0-9][0-9][0-9][0-9][0-9][0-9][Zz])"
        mts = re.match(pattern0, jsummary)
        if mts:
            what = mts.group(1)
            ts = mts.group(3)
        else:
            what = "0"
            ts = "0"
        # print("ts:",ts)
        # print(jdesc)
        jlines = jdesc.splitlines()
        lm = iter(jlines)
        pattern1 = re.compile("(.*)tract in (.*)")
        pattern2 = re.compile("(.*)exposure >=([0-9]*) and exposure <=( *[0-9]*)")
        pattern2b = re.compile("(.*)visit >=([0-9]*) and visit <=( *[0-9]*)")
        pattern2a = re.compile(
            "(.*)detector>=([0-9]*).*exposure >=( *[0-9]*) and exposure <=( *[0-9]*)"
        )
        pattern3 = re.compile(
            "(.*)Status:.*nTasks:(.*)nFiles:(.*)nRemain.*nProc: nFinish:(.*) nFail:(.*) nSubFinish:(.*)"
        )
        # pattern3=re.compile("(.*)Status:(.*)")
        pattern4 = re.compile("(.*)PanDA.*link:(.*)")
        hilow = "()"
        status = [0, 0, 0, 0, 0]
        pandalink = ""
        for ls in lm:
            n1 = pattern1.match(ls)
            if n1:
                # print("Tract range:",n1.group(2),":end")
                hilow = n1.group(2)
                # print("hilow:",hilow)
            n2 = pattern2.match(ls)
            if n2:
                print("exposurelo:", n2.group(2), " exphigh:", n2.group(3), ":end")
                hilow = "(" + str(int(n2.group(2))) + "," + str(int(n2.group(3))) + ")"
                # print("hilow:",hilow)
            # else:
            n2b = pattern2b.match(ls)
            if n2b:
                print("visitlo:", n2b.group(2), " visthigh:", n2b.group(3), ":end")
                hilow = "(" + str(int(n2b.group(2))) + "," + str(int(n2b.group(3))) + ")"
            # print("no match to l",l)
            n2a = pattern2a.match(ls)
            if n2a:
                print(
                    "detlo",
                    n2a.group(2),
                    "exposurelo:",
                    n2a.group(3),
                    " exphigh:",
                    n2a.group(4),
                    ":end",
                )
                hilow = (
                    "("
                    + str(int(n2a.group(3)))
                    + ","
                    + str(int(n2a.group(4)))
                    + ")d"
                    + str(int(n2a.group(2)))
                )
            n3 = pattern3.match(ls)
            if n3:
                # print("match is",n3.group(1),n3.group(2))
                # print("(.*)Status: finished nTasks:(.*)nFiles:(.*)nRemain:(.*)nProc: nFinish:(.*) nFail:(.*) nSubFinish:(.*)")
                # sys.exit(1)
                statNtasks = int(n3.group(2))
                statNfiles = int(n3.group(3))
                statNFinish = int(n3.group(4))
                statNFail = int(n3.group(5))
                statNSubFin = int(n3.group(6))
                # print("Job status tasks,files,finish,fail,subfin:",statNtasks,statNfiles,statNFinish,statNFail,statNSubFin)
                status = [statNtasks, statNfiles, statNFinish, statNFail, statNSubFin]
            m = pattern4.match(ls)
            if m:
                pandalink = m.group(2)
                # print("pandalink:",pandaline)

        # sys.exit(1)

        return ts, status, hilow, pandalink, what

    @staticmethod
    def dict_to_table(in_dict, sorton):
        """

        :param in_dict:
        :param sorton:
        :return:
        """
        dictheader = ["Date", "PREOPS", "STATS", "(T,Q,D,Fa,Sf)", "PANDA", "DESCRIP"]

        table_out = "||"
        for i in dictheader:
            table_out += str(i) + "||"
        table_out += "\n"

        # sortbydescrip=sorted(in_dict[3])
        for i in sorted(in_dict.keys(), reverse=True):
            pis = i.split("#")[0]
            ts = i.split("#")[1]
            status = in_dict[i][2]
            nT = status[0]
            nFile = status[1]
            nFin = status[2]
            nFail = status[3]
            nSubF = status[4]
            statstring = (
                str(nT)
                + ","
                + str(nFile)
                + ","
                + str(nFin)
                + ","
                + str(nFail)
                + ","
                + str(nSubF)
            )
            scolor = "black"
            # print(statstring,nT,nFile,nFin,nFail,nSubF)
            if nFail > 0:
                scolor = "red"
            if nT == nFin + nSubF:
                scolor = "black"
            if nT == nFin:
                scolor = "green"
            if int(nFail) == 0 and int(nFile) == 0:
                scolor = "blue"

            longdatetime = ts
            shortyear = str(longdatetime[0:4])
            shortmon = str(longdatetime[4:6])
            shortday = str(longdatetime[6:8])
            # print(shortyear,shortmon,shortday)

            what = in_dict[i][4]
            if len(what) > 28:
                what = what[0:28]

            table_out += (
                "| "
                + str(shortyear)
                + "-"
                + str(shortmon)
                + "-"
                + str(shortday)
                + " | ["
                + str(in_dict[i][0])
                + "|https://jira.lsstcorp.org/browse/"
                + str(in_dict[i][0])
                + "] | "
                + str(in_dict[i][1])
                + "|{color:"
                + scolor
                + "}"
                + statstring
                + "{color} | [pDa|"
                + in_dict[i][3]
                + "] |"
                + str(what)
                + "|\n"
            )
        return table_out

    @staticmethod
    def dict_to_table1(in_dict, sorton):
        """

        :param in_dict:
        :param sorton:
        :return:
        """
        dictheader = ["Date", "PREOPS", "STATS", "(T,Q,D,Fa,Sf)", "PANDA", "DESCRIP"]

        table_out = "||"
        for i in dictheader:
            table_out += str(i) + "||"
        table_out += "\n"

        for i in sorted(in_dict.keys(), reverse=True):
            pis = i.split("#")[0]
            # print("pis is:",pis)
            stepstring = in_dict[i][4]
            stepstart = stepstring[0:5]
            # print("stepstart is:",stepstart)
            if stepstart == "step1":
                ts = i.split("#")[1]
                status = in_dict[i][2]
                nT = status[0]
                nFile = status[1]
                nFin = status[2]
                nFail = status[3]
                nSubF = status[4]
                statstring = (
                    str(nT)
                    + ","
                    + str(nFile)
                    + ","
                    + str(nFin)
                    + ","
                    + str(nFail)
                    + ","
                    + str(nSubF)
                )
                scolor = "black"
                if nFail > 0:
                    scolor = "red"
                if nT == nFin + nSubF:
                    scolor = "black"
                if nT == nFin:
                    scolor = "green"
                if nFail == 0 and nFile == 0:
                    scolor = "blue"

                longdatetime = ts
                shortyear = str(longdatetime[0:4])
                shortmon = str(longdatetime[4:6])
                shortday = str(longdatetime[6:8])
                # print(shortyear,shortmon,shortday)

                what = in_dict[i][4]
                if len(what) > 25:
                    what = what[0:25]
                table_out += (
                    "| "
                    + str(shortyear)
                    + "-"
                    + str(shortmon)
                    + "-"
                    + str(shortday)
                    + " | ["
                    + str(in_dict[i][0])
                    + "|https://jira.lsstcorp.org/browse/"
                    + str(in_dict[i][0])
                    + "] | "
                    + str(in_dict[i][1])
                    + "|{color:"
                    + scolor
                    + "}"
                    + statstring
                    + "{color} | [pDa|"
                    + in_dict[i][3]
                    + "] |"
                    + str(what)
                    + "|\n"
                )
        return table_out

    def drp_add_job_to_summary(
        self, first, ts, pissue, jissue, status, frontend, frontend1, backend
    ):
        """

        :param first:
        :param ts:
        :param pissue:
        :param jissue:
        :param status:
        :param frontend:
        :param frontend1:
        :param backend:
        :return:
        """
        #        ju = JiraUtils()
        #        ajira, username = self.ju.get_login()
        backendissue = self.ajira.issue(backend)
        olddescription = backendissue.fields.description

        frontendissue = self.ajira.issue(frontend)
        frontendissue1 = self.ajira.issue(frontend1)

        jissue = self.ajira.issue(jissue)
        jdesc = jissue.fields.description
        jsummary = jissue.fields.summary
        print("summary is", jsummary)
        ts, status, hilow, pandalink, what = self.parse_issue_desc(jdesc, jsummary)
        print(
            "new entry (ts,status,hilow,pandalink,step)",
            ts,
            status,
            hilow,
            pandalink,
            what,
        )

        if first == 1:
            a_dict = dict()
        else:
            a_dict = json.loads(olddescription)

        if first == 2:
            print("removing PREOPS, DRP", str(pissue), str(jissue))
            for key, value in a_dict.items():
                # print("key",key,"value",value)
                if value[1] == str(jissue) and value[0] == str(pissue):
                    print("removing one key with:", str(jissue), str(pissue))
                    del a_dict[key]
                    break
        else:
            a_dict[str(pissue) + "#" + str(ts)] = [
                str(pissue),
                str(jissue),
                status,
                pandalink,
                what + str(hilow),
            ]

        newdesc = self.dict_to_table(a_dict, -1)
        frontendissue.update(fields={"description": newdesc})

        newdesc1 = self.dict_to_table1(a_dict, -1)
        frontendissue1.update(fields={"description": newdesc1})

        newdict = json.dumps(a_dict)
        backendissue.update(fields={"description": newdict})
        print("Summary updated, see DRP-55 or DRP-53")

    @staticmethod
    def make_prod_groups(template, band, groupsize, skipgroups, ngroups, explist):
        """

        :param template:
        :param band:
        :param groupsize:
        :param skipgroups:
        :param ngroups:
        :param explist:
        :return:
        """
        f = open(explist, "r")
        tempstr = os.path.basename(template)
        patt = re.compile("(.*).yaml")
        m = patt.match(tempstr)
        if m:
            outtemp = m.group(1)
        else:
            outtemp = tempstr

        done = 0
        lastgroup = skipgroups + ngroups

        highexp = 0
        lowexp = 0
        groupcount = int(0)
        totcount = int(0)
        expnum = 0
        for lm in f:
            line = lm.strip()
            if line == "":
                break
            a = line.split()
            aband = str(a[0])
            expnum = int(a[1])
            if aband != band and band != "all" and band != "f":
                continue
            groupcount += 1
            totcount += 1
            curgroup = int(totcount / groupsize)
            curcount = totcount % groupsize
            if curcount == 1:
                lowexp = expnum
            if curcount == 0 and skipgroups < curgroup <= lastgroup:
                highexp = expnum
                com = (
                    "sed -e s/BAND/"
                    + str(band)
                    + "/g -e s/GNUM/"
                    + str(int(curgroup))
                    + "/g -e s/LOWEXP/"
                    + str(lowexp)
                    + "/g -e s/HIGHEXP/"
                    + str(highexp)
                    + "/g "
                    + str(template)
                    + " >"
                    + str(outtemp)
                    + "_"
                    + str(band)
                    + "_"
                    + str(int(curgroup))
                    + ".yaml"
                )
                return_val = subprocess.call(com, shell=True)
                print(com + " " + str(return_val))
        # lastgroup
        curgroup = int(totcount / groupsize) + 1
        curcount = totcount % groupsize
        if curcount != 0 and skipgroups < curgroup <= lastgroup:
            highexp = expnum
            com = (
                "sed -e s/BAND/"
                + str(band)
                + "/g -e s/GNUM/"
                + str(int(curgroup))
                + "/g -e s/LOWEXP/"
                + str(lowexp)
                + "/g -e s/HIGHEXP/"
                + str(highexp)
                + "/g "
                + str(template)
                + " >"
                + str(outtemp)
                + "_"
                + str(band)
                + "_"
                + str(int(curgroup))
                + ".yaml"
            )
            return_val = subprocess.call(com, shell=True)
            print(com + " " + str(return_val))

    def drp_issue_update(self, bpsyamlfile, pissue, drpi, ts):
        """

        :param bpsyamlfile:
        :param pissue:
        :param drpi:
        :param ts:
        :return:
        """
        bpsstr, kwd, akwd, pupn = self.parse_yaml(bpsyamlfile, ts)
        print("pupn:", pupn)
        year = str(pupn[0:4])
        month = str(pupn[4:6])
        # day=str(pupn[6:8])
        day = str("01")
        print("year:", year)
        print("year:", month)
        print("year:", day)
        a_link = (
            "https://panda-doma.cern.ch/tasks/?taskname=*"
            + pupn.lower()
            + "*&date_from="
            + str(day)
            + "-"
            + str(month)
            + "-"
            + str(year)
            + "&days=62&sortby=time-ascending"
        )

        print("link:", a_link)
        dobut = 0
        dopan = 0
        print(dobut, dopan)
        # print(totmaxmem,nquanta,pnquanta)

        nowut = (
            datetime.datetime.now(timezone("GMT")).strftime("%Y-%m-%d %H:%M:%S") + "Z"
        )
        print(bpsstr, kwd, akwd)

        upn = kwd["campaign"] + "/" + pupn
        # upn.replace("/","_")
        # upn=d['bps_defined']['uniqProcName']
        stepname = kwd["pipelineYaml"]
        p = re.compile("(.*)#(.*)")
        m = p.match(stepname)
        print("stepname " + stepname)
        if m:
            steppath = m.group(1)
            stepcut = m.group(2)
        else:
            stepcut = ""

        print("steplist " + stepcut)
        print("steppath " + steppath)
        bpsstr += "pipelineYamlSteps: " + stepcut + "\n{code}\n"

        print(upn + "#" + stepcut)
        sl = self.parse_drp(steppath, stepcut)
        tasktable = (
            "Butler Statistics\n"
            + "|| Step || Task || Start || nQ || sec/Q || sum(hr) || maxGB ||"
            + "\n"
        )
        for s in sl:
            if dobut == 0 or s[1] not in nquanta.keys():
                print("skipping:", s[0])
                tasktable += (
                    "|"
                    + s[0]
                    + "|"
                    + s[1]
                    + "|"
                    + " "
                    + "|"
                    + " "
                    + "|"
                    + " "
                    + "|"
                    + " "
                    + "|"
                    + " "
                    + "|"
                    + "\n"
                )
            else:
                tasktable += (
                    "|"
                    + s[0]
                    + "|"
                    + s[1]
                    + "|"
                    + str(startdate[s[1]])
                    + "|"
                    + str(nquanta[s[1]])
                    + "|"
                    + str("{:.1f}".format(secperstep[s[1]]))
                    + "|"
                    + str("{:.1f}".format(sumtime[s[1]]))
                    + "|"
                    + str("{:.2f}".format(maxmem[s[1]]))
                    + "| \n"
                )

        if dobut == 1:
            tasktable += (
                "Total core-hours: "
                + str("{:.1f}".format(totsumsec))
                + " Peak Memory (GB): "
                + str("{:.1f}".format(totmaxmem))
                + "\n"
            )
        tasktable += "\n"
        print(tasktable)

        tasktable += "PanDA PREOPS: " + str(pissue) + " link:" + a_link + "\n"
        if dopan == 1:
            tasktable += (
                "Panda Statistics as of: "
                + nowut
                + "\n"
                + "|| Step || Task || Start || PanQ || Psec/Q || wall(hr) || Psum(hr) ||parall cores||"
                + "\n"
            )
            for s in sl:
                if dopan == 0 or s[1] not in pnquanta.keys():
                    tasktable += (
                        "|"
                        + s[0]
                        + "|"
                        + s[1]
                        + "|"
                        + " "
                        + "|"
                        + " "
                        + "|"
                        + " "
                        + "|"
                        + " "
                        + "|"
                        + " "
                        + "|"
                        + " "
                        + "|"
                        + "\n"
                    )
                else:
                    tasktable += (
                        "|"
                        + s[0]
                        + "|"
                        + s[1]
                        + "|"
                        + str(pstartdate[s[1]])
                        + "|"
                        + str(pnquanta[s[1]])
                        + "|"
                        + str("{:.1f}".format(psecperstep[s[1]]))
                        + "|"
                        + str("{:.1f}".format(pwallhr[s[1]]))
                        + "|"
                        + str("{:.2f}".format(psumtime[s[1]]))
                        + "|"
                        + str("{:.0f}".format(pmaxmem[s[1]]))
                        + "| \n"
                    )

        # (ptotmaxmem,ptotsumsec,pnquanta,psecperstep,wallhr,sumtime,maxmem,pupn,pstat,pntasks,pnfiles,pnremain,pnproc,pnfin,pnfail,psubfin)=parsepandatable(panstepfile)

        if dopan == 1:
            tasktable += (
                "Total wall-hours: "
                + str("{:.1f}".format(ptotmaxmem))
                + " Total core-hours: "
                + str("{:.1f}".format(ptotsumsec))
                + "\n"
            )
            tasktable += (
                "Status:"
                + str(pstat)
                + " nTasks:"
                + str(pntasks)
                + " nFiles:"
                + str(pnfiles)
                + " nRemain:"
                + str(pnproc)
                + " nProc:"
                + " nFinish:"
                + str(pnfin)
                + " nFail:"
                + str(pnfail)
                + " nSubFinish:"
                + str(psubfin)
                + "\n"
            )
        tasktable += "\n"
        print(tasktable)
        # (totmaxmem,totsumsec,nquanta,secperstep,sumtime,maxmem)=parsebutlertable(butstepfile)

        if drpi == "DRP0":
            issue = self.ajira.create_issue(
                project="DRP",
                issuetype="Task",
                summary="a new issue",
                description=bpsstr + tasktable,
                components=[{"name": "Test"}],
            )
        else:
            issue = self.ajira.issue(drpi)
        issue.update(
            fields={"summary": stepcut + "#" + upn, "description": bpsstr + tasktable}
        )
        print("issue:" + str(issue))
