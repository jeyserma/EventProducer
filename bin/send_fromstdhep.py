#!/usr/bin/env python3
import os, sys
import subprocess
import time
import yaml
import glob
from select import select
import EventProducer.common.utils as ut
import EventProducer.common.makeyaml as my

class send_fromstdhep():

#__________________________________________________________
    def __init__(self,njobs, events, process, islsf, iscondor, islocal, queue, priority, ncpus, para, version, detector, decay):  #, pycard):
        self.njobs    = njobs
        self.events   = events
        self.process  = process
        self.islsf    = islsf
        self.iscondor = iscondor
        self.islocal  = islocal
        self.queue    = queue
        self.priority = priority
        self.ncpus    = ncpus
        self.para     = para
        self.version  = version
        self.decay    = decay
        self.detector = detector
        #self.pycard   = pycard
        self.user     = os.environ['USER']


#__________________________________________________________
    def send(self, force):

        Dir = os.getcwd()
    
        gplist=self.para.gridpacklist
        outdir='%s%s/%s/'%(self.para.delphes_dir,self.version,self.detector)

        try:
            gplist[self.process]
        except KeyError as e:
            print ('process %s does not exist as gridpack'%self.process)
            sys.exit(3)

        acctype='FCC'
        if 'FCCee' in self.para.module_name:  acctype='FCCee'

        logdir=Dir+"/BatchOutputs/%s/%s/%s/%s/"%(acctype,self.version,self.detector,self.process)
        if not ut.dir_exist(logdir):
            os.system("mkdir -p %s"%logdir)

        #if not self.islocal:
            #yamldir = '%s/%s/%s/%s'%(self.para.yamldir,self.version,self.detector,self.process)
            #if not ut.dir_exist(yamldir):
                #os.system("mkdir -p %s"%yamldir)

        delphescards_base = '%scard_%s.tcl'%(self.para.delphescards_dir,self.detector)
        delphescards_base=delphescards_base.replace('_VERSION_',self.version)
        if ut.file_exist(delphescards_base)==False:
            print ('delphes card does not exist: ',delphescards_base,' , exit')
            sys.exit(3)

        if self.islsf==False and self.iscondor==False and self.islocal==False:
            print ("Submit issue : LSF nor CONDOR not Local flag defined !!!")
            sys.exit(3)


        #fccconfig = '%s%s'%(self.para.fccconfig_dir,self.para.fccconfig)
        #if ut.file_exist(fccconfig)==False:
            #print ('fcc config file does not exist: ',fccconfig)
            #sys.exit(3)
#

        print ('======================================',self.process)
        
        '''

        pr_noht=''
        if '_HT_' in self.process:
            ssplit=self.process.split('_')
            stest=''
            for s in xrange(0,len(ssplit)-3):
                stest+=ssplit[s]+'_'
            pr_noht= stest[0:len(stest)-1]

        #check that the specified decay exists
        if self.process in self.para.decaylist and self.decay != '' and '_HT_' not in self.process:
            if self.decay not in self.para.decaylist[self.process]:
                print 'decay ==%s== does not exist for process ==%s=='%(self.decay,self.process)
                sys.exit(3)

        #check that the specified decay exists
        if pr_noht in self.para.decaylist and self.decay != '' and '_HT_' in self.process:
            if self.decay not in self.para.decaylist[pr_noht]:
                print 'decay ==%s== does not exist for process ==%s=='%(self.decay,self.process)
                sys.exit(3)
        '''


        #pythiacard=self.para.pythiacards_dir+'/'+self.pycard

        #if not os.path.isfile(pythiacard):
            #print ('{} does not exist'.format(pythiacard))
            #sys.exit(3)

        pr_decay = self.process       
        if self.process in self.para.decaylist and self.decay != '':
            pr_decay=self.process
            print ('====',pr_decay,'====')
            pr_decay=self.process+'_'+self.decay


        # first string before underscore is generator
        #mcprg_str = pr_decay.split('_')[0]
        #processp8 = pr_decay.replace(mcprg_str, mcprg_str+'p8')

        #print (processp8)

        #acctype='FCC'
        #if 'HELHC' in self.para.module_name:  acctype='HELHC'
        #elif 'FCCee' in self.para.module_name:  acctype='FCCee'

        #logdir=Dir+"/BatchOutputs/%s/%s/%s/"%(acctype,self.version,processp8)
        #if not ut.dir_exist(logdir):
            #os.system("mkdir -p %s"%logdir)

        if not self.islocal:
            yamldir = '%s/%s/%s/%s'%(self.para.yamldir,self.version,self.detector,self.process)
            if not ut.dir_exist(yamldir):
                os.system("mkdir -p %s"%yamldir)


        training=False
        if 'training' in self.version: training=True

        yamlstdhepdir = '%s/stdhep/%s'%(self.para.yamldir,self.process)
        if training:
            yamlstdhepdir = '%s/stdhep/training/%s'%(self.para.yamldir,self.process)
        if not( 'spring2021' in self.version or 'pre_fall2022' in self.version or 'dev' in self.version ):   # winter2023 and later:
            prodtag = self.version.replace("_training","")
            yamlstdhepdir = '%s/stdhep/%s/%s'%(self.para.yamldir,prodtag,self.process)
            if training:
                 yamlstdhepdir = '%s/stdhep/%s/training/%s'%(self.para.yamldir,prodtag,self.process)
        print("yamlstdhepdir = ",yamlstdhepdir)
  
        All_files = glob.glob("%s/events_*.yaml"%yamlstdhepdir)
        if len(All_files)==0:
            print ('there is no STDHEP files checked for process %s exit'%self.process)
            sys.exit(3)

        if len(All_files)<self.njobs:
            print ('only %i STDHEP file exists, will not run all the jobs requested'%len(All_files))

        nbjobsSub=0
        ntmp=0

        if self.islsf==False and self.iscondor==False and self.islocal==False:
            print ("Submit issue : LSF nor CONDOR flag defined !!!")
            sys.exit(3)

        condor_file_str=''
        

        for i in range(len(All_files)):

            if nbjobsSub == self.njobs: break
            
            tmpf=None
            with open(All_files[i], 'r') as stream:
                try:
                    #tmpf = yaml.load(stream, Loader=yaml.FullLoader)
                    tmpf = yaml.load(stream, Loader=yaml.FullLoader)
                    if ut.getsize(All_files[i])==0:continue
                    if tmpf['processing']['status']!='DONE': continue
                    
                except yaml.YAMLError as exc:
                    print(exc)

            jobid=tmpf['processing']['jobid']
            print("jobid =",jobid)

            if not self.islocal:
                print("yamldir =",yamldir)
               
                myyaml = my.makeyaml(yamldir, jobid)
                print ("myyaml =",myyaml)
                if not myyaml: 
                    print ('job %s already exists'%jobid)
                    continue

                outfile='%s/%s/events_%s.root'%(outdir,self.process,jobid)
                if ut.file_exist(outfile):
                    print ('outfile already exist, continue  ',outfile)

            if self.islocal:
                outfile='%s/events_%s.root'%(logdir,jobid)
                if ut.file_exist(outfile):
                    print ('file %s already locally exist, continue'%outfile)
                    continue


            frunname = 'job%s.sh'%(jobid)
            frunfull = '%s/%s'%(logdir,frunname)

            frun = None
            try:
                frun = open(frunfull, 'w')
            except IOError as e:
                print ("I/O error({0}): {1}".format(e.errno, e.strerror))
                time.sleep(10)
                frun = open(frunfull, 'w')

            subprocess.getstatusoutput('chmod 777 %s'%(frunfull))
            frun.write('#!/bin/bash\n')
            frun.write('unset LD_LIBRARY_PATH\n')
            frun.write('unset PYTHONHOME\n')
            frun.write('unset PYTHONPATH\n')
            frun.write('source %s\n'%(self.para.prodTag[self.version]))
            frun.write('mkdir job%s_%s\n'%(jobid,self.process))
            frun.write('cd job%s_%s\n'%(jobid,self.process))
            frun.write('export EOS_MGM_URL=\"root://eospublic.cern.ch\"\n')
            #frun.write('mkdir -p %s%s/%s\n'%(self.para.delphes_dir,self.version,processp8))
            if self.islocal==False:
                frun.write('mkdir -p %s/%s\n'%(outdir,self.process))

            frun.write('xrdcp %s .\n'%(tmpf['processing']['out']))
            frun.write('gunzip -c %s > events.stdhep\n'%tmpf['processing']['out'].split('/')[-1])          

            frun.write('xrdcp %s card.tcl\n'%(delphescards_base))
            frun.write('xrdcp /home/submit/jaeyserm/fccee/FCC-config/%s/FCCee/Delphes/edm4hep_%s.tcl edm4hep.tcl\n'%(self.version,self.detector))

            frun.write('DelphesSTDHEP_EDM4HEP card.tcl edm4hep.tcl  events_%s.root  events.stdhep \n'%(jobid))

            xrdcp = True
            #frun.write('xrdcp events_%s.root %s\n'%(jobid,outfile))
            if xrdcp:
                frun.write('xrdcp events_%s.root %s\n'%(jobid,outfile.replace("/data/submit/cms/store/", "root://submit55.mit.edu//store/")))
            else:
                frun.write('xrdcp proc.stdhep.gz %s\n'%(outfile))

            frun.write('cd ..\n')
            frun.write('rm -rf job%s_%s\n'%(jobid,self.process))
            frun.close()

            if self.islsf==True :
              cmdBatch="bsub -M 3000000 -R \"pool=40000\" -q %s -o %s -cwd %s %s" %(self.queue, logdir+'/job%s/'%(jobid), logdir+'/job%s/'%(jobid),frunfull)

              batchid=-1
              job,batchid=ut.SubmitToLsf(cmdBatch,10,"%i/%i"%(nbjobsSub,self.njobs))
              nbjobsSub+=job
            elif self.iscondor==True :
              condor_file_str+=frunfull+" "
              nbjobsSub+=1

            elif self.islocal==True:
                print ('will run locally')
                nbjobsSub+=1
                os.system('%s'%frunfull)


        if self.iscondor==True :
            # clean string
            condor_file_str=condor_file_str.replace("//","/")
            #
            frunname_condor = 'job_desc_stdhep.cfg'
            frunfull_condor = '%s/%s'%(logdir,frunname_condor)
            frun_condor = None
            try:
                frun_condor = open(frunfull_condor, 'w')
            except IOError as e:
                print ("I/O error({0}): {1}".format(e.errno, e.strerror))
                time.sleep(10)
                frun_condor = open(frunfull_condor, 'w')
            subprocess.getstatusoutput('chmod 777 %s'%frunfull_condor)
            #
            frun_condor.write('executable     = $(filename)\n')
            frun_condor.write('Log            = %s/condor_job.%s.$(ClusterId).$(ProcId).log\n'%(logdir,str(jobid)))
            frun_condor.write('Output         = %s/condor_job.%s.$(ClusterId).$(ProcId).out\n'%(logdir,str(jobid)))
            frun_condor.write('Error          = %s/condor_job.%s.$(ClusterId).$(ProcId).error\n'%(logdir,str(jobid)))
            frun_condor.write('getenv         = True\n')
            frun_condor.write('environment    = "LS_SUBCWD=%s"\n'%logdir) # not sure
            #frun_condor.write('requirements   = ( (OpSysAndVer =?= "CentOS7") && (Machine =!= LastRemoteHost) )\n')
            #frun_condor.write('requirements   = ( (OpSysAndVer =?= "SLCern6") && (Machine =!= LastRemoteHost) )\n')
            #frun_condor.write('requirements    = ( (OpSysAndVer =?= "CentOS7") && (Machine =!= LastRemoteHost) && (TARGET.has_avx2 =?= True) )\n')
            frun_condor.write('requirements   = ( BOSCOCluster =!= "t3serv008.mit.edu" && BOSCOCluster =!= "ce03.cmsaf.mit.edu" && BOSCOCluster =!= "eofe8.mit.edu")\n')
            frun_condor.write('+DESIRED_Sites = "mit_tier3"\n')

            frun_condor.write('on_exit_remove = (ExitBySignal == False) && (ExitCode == 0)\n')
            frun_condor.write('max_retries    = 3\n')
            frun_condor.write('+JobFlavour    = "%s"\n'%self.queue)
            frun_condor.write('+AccountingGroup = "%s"\n'%self.priority)
            frun_condor.write('RequestCpus = %s\n'%self.ncpus)

            frun_condor.write('use_x509userproxy = True\n')
            frun_condor.write('x509userproxy = /home/submit/jaeyserm/x509up_u204569\n')

            frun_condor.write('queue filename matching files %s\n'%condor_file_str)
            frun_condor.close()
            #
            nbjobsSub=0
            cmdBatch="condor_submit %s"%frunfull_condor
            print (cmdBatch)
            job=ut.SubmitToCondor(cmdBatch,10,"%i/%i"%(nbjobsSub,self.njobs))
            nbjobsSub+=job

        print ('succesfully sent %i  job(s)'%nbjobsSub)


