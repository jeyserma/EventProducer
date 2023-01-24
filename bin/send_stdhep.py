#!/usr/bin/env python3
import os, sys
import subprocess
import time
import EventProducer.common.utils as ut
import EventProducer.common.makeyaml as my

class send_stdhep():

#__________________________________________________________
    def __init__(self,njobs,events, process, islsf, iscondor, islocal, queue, priority, ncpus, para, version, typestdhep, training):
        self.njobs    = njobs
        self.events   = events
        self.process  = process
        self.islsf    = islsf
        self.iscondor = iscondor
        self.islocal  = islocal
        self.queue    = queue
        self.priority = priority
        self.ncpus    = ncpus
        self.user     = os.environ['USER']
        self.para     = para
        self.version  = version
        self.typestdhep  = typestdhep
        self.training = training

#__________________________________________________________
    def send(self):
        Dir = os.getcwd()
        nbjobsSub=0

        print("njobs=",self.njobs)
        xrdcp = True

        stdhepdir=self.para.stdhep_dir
        # From winter2023 onwards, the stdhep files are store in stdehp/prodTag or stdhep/prodTag/training
        if not( 'spring2021' in self.version or 'pre_fall2022' in self.version or 'dev' in self.version ):
            prodtag = self.version.replace("_training","")
            stdhepdir=stdhepdir+"/%s/"%(prodtag)
        if self.typestdhep == 'wzp6' and self.training:
            stdhepdir = stdhepdir + "training/"
        print("stdhepdir =",stdhepdir)
        gpdir=self.para.gp_dir

        acctype='FCC'
        if 'HELHC' in self.para.module_name:  acctype='HELHC'
        elif 'FCCee' in self.para.module_name:  acctype='FCCee'

        logdir=Dir+"/BatchOutputs/%s/stdhep/%s"%(acctype,self.process)
        if not ut.dir_exist(logdir):
            os.system("mkdir -p %s"%logdir)


        if not self.islocal:
             yamldir = '%s/stdhep/%s'%(self.para.yamldir,self.process)    # for pre-winter2023 tags
             if self.training:
                  yamldir = '%s/stdhep/training/%s'%(self.para.yamldir,self.process)
             if not( 'spring2021' in self.version or 'pre_fall2022' in self.version or 'dev' in self.version ):   # winter2023 and later:
                  prodtag = self.version.replace("_training","")
                  yamldir = '%s/stdhep/%s/%s'%(self.para.yamldir,prodtag,self.process)
                  if self.training:
                      yamldir = '%s/stdhep/%s/training/%s'%(self.para.yamldir,prodtag,self.process)
             print("yamldir = ",yamldir)
             if not ut.dir_exist(yamldir):
                 os.system("mkdir -p %s"%yamldir)

        if self.typestdhep == 'wzp6':
            whizardcard='%s%s.sin'%(self.para.whizardcards_dir, 'v3.0.3/'+self.process)     # Whizard 2.8.5, with Pythia6 interface
            if 'spring2021' in self.version or 'pre_fall2022' in self.version or 'dev' in self.version:
                whizardcard='%s%s.sin'%(self.para.whizardcards_dir, 'v2.8.5/'+self.process)     # Whizard 2.8.5, with Pythia6 interface
	  

        whizardcard=whizardcard.replace('_VERSION_',self.version)
        if ut.file_exist(whizardcard)==False:
            print ('Whizard card does not exist: ',whizardcard,' , exit')
            if '_EvtGen_' not in self.process:
                sys.exit(3)


        if self.islsf==False and self.iscondor==False and self.islocal==False:
            print ("Submit issue : LSF nor CONDOR nor Local flag defined !!!")
            sys.exit(3)

        if self.islocal==False:
            os.system('mkdir -p %s%s\n'%(stdhepdir,self.process))

        condor_file_str=''
        while nbjobsSub<self.njobs:
            #uid = int(ut.getuid(self.user))
            if self.typestdhep == 'wzp6':
                uid = ut.getuid2(self.user)
                if self.training: 
                      #print("---- INFO: using getuidtraining")
                      uid = ut.getuidtraining(self.user)

            if not self.islocal:
                myyaml = my.makeyaml(yamldir, uid)
                if not myyaml: 
                    print ('job %s already exists'%uid)
                    continue

                outfile='%s/%s/events_%s.stdhep.gz'%(stdhepdir,self.process,uid)
                if ut.file_exist('%s/%s/events_%s.stdhep.gz'%(stdhepdir,self.process,uid)):
                    print ('already exist, continue')
                    continue

            if self.islocal:
                outfile = '%s/events_%s.stdhep.gz'%(logdir,uid)
                if ut.file_exist(outfile):
                    print ('file %s already locally exist, continue'%outfile)
                    continue


            frunname = 'job%s.sh'%(uid) 
            frunfull = '%s/%s'%(logdir,frunname)

            frun = None
            try:
                frun = open(frunfull, 'w')
            except IOError as e:
                print ("I/O error({0}): {1}".format(e.errno, e.strerror))
                time.sleep(10)
                frun = open(frunfull, 'w')
                
            subprocess.getstatusoutput('chmod 777 %s'%frunfull)
            frun.write('#!/bin/bash\n')
            frun.write('unset LD_LIBRARY_PATH\n')
            frun.write('unset PYTHONHOME\n')
            frun.write('unset PYTHONPATH\n')
            frun.write('mkdir job%s_%s\n'%(uid,self.process))
            frun.write('cd job%s_%s\n'%(uid,self.process))
            frun.write('source %s\n'%(self.para.defaultstack))
            
            
            
            frun.write('xrdcp %s thecard.sin\n'%(whizardcard))
            
            frun.write('echo "n_events = %i" > header.sin \n'%(self.events))
            frun.write('echo "seed = %s"  >> header.sin \n'%(uid))
            frun.write('cat header.sin thecard.sin > card.sin \n') 

            frun.write('whizard card.sin \n')
            frun.write('echo "finished run"\n')
            frun.write('gzip proc.stdhep \n')
            #frun.write('python /afs/cern.ch/work/f/fccsw/public/FCCutils/eoscopy.py events.lhe.gz %s/%s/events_%s.lhe.gz\n'%(lhedir,self.process ,uid))
            #frun.write('xrdcp -N -v proc.stdhep.gz root://eospublic.cern.ch/%s/%s/events_%s.lhe.gz\n'%(stdhepdir,self.process ,uid))
            #frun.write('python /home/submit/jaeyserm/fccee/EventProducer/filecopy.py proc.stdhep.gz %s\n'%(outfile))
            if xrdcp:
                frun.write('xrdcp proc.stdhep.gz %s\n'%(outfile.replace("/data/submit/cms/store/", "root://submit55.mit.edu//store/")))  
            else:
                frun.write('xrdcp proc.stdhep.gz %s\n'%(outfile))
            frun.write('echo "stdhep.gz file successfully copied on eos"\n')

            frun.write('cd ..\n')
            #frun.write('rm -rf job%s_%s\n'%(uid,self.process))
            frun.close()

            if self.islsf==True :
              cmdBatch="bsub -M 2000000 -R \"rusage[pool=2000]\" -q %s -o %s -cwd %s %s" %(self.queue,logdir+'/job%s/'%(uid),logdir+'/job%s/'%(uid),logdir+'/'+frunname)
              #print cmdBatch

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
            frun_condor.write('Log            = %s/condor_job.%s.$(ClusterId).$(ProcId).log\n'%(logdir,str(uid)))
            frun_condor.write('Output         = %s/condor_job.%s.$(ClusterId).$(ProcId).out\n'%(logdir,str(uid)))
            frun_condor.write('Error          = %s/condor_job.%s.$(ClusterId).$(ProcId).error\n'%(logdir,str(uid)))
            frun_condor.write('getenv         = True\n')
            frun_condor.write('environment    = "LS_SUBCWD=%s"\n'%logdir) # not sure
            #frun_condor.write('requirements   = ( (OpSysAndVer =?= "CentOS7") && (Machine =!= LastRemoteHost) )\n')
            #frun_condor.write('requirements   = ( (OpSysAndVer =?= "SLCern6") && (Machine =!= LastRemoteHost) )\n')
            #frun_condor.write('requirements    = ( (OpSysAndVer =?= "CentOS7") && (Machine =!= LastRemoteHost) && (TARGET.has_avx2 =?= True) )\n')
            frun_condor.write('requirements   = ( BOSCOCluster =!= "t3serv008.mit.edu" && BOSCOCluster =!= "ce03.cmsaf.mit.edu" && BOSCOCluster =!= "eofe8.mit.edu")\n')

            #frun_condor.write('+DESIRED_Sites = "mit_tier3"\n')
            #frun_condor.write('+DESIRED_Sites = "mit_tier2"\n')
            frun_condor.write('+DESIRED_Sites = "T2_AT_Vienna,T2_BE_IIHE,T2_BE_UCL,T2_BR_SPRACE,T2_BR_UERJ,T2_CH_CERN,T2_CH_CERN_AI,T2_CH_CERN_HLT,T2_CH_CERN_Wigner,T2_CH_CSCS,T2_CH_CSCS_HPC,T2_CN_Beijing,T2_DE_DESY,T2_DE_RWTH,T2_EE_Estonia,T2_ES_CIEMAT,T2_ES_IFCA,T2_FI_HIP,T2_FR_CCIN2P3,T2_FR_GRIF_IRFU,T2_FR_GRIF_LLR,T2_FR_IPHC,T2_GR_Ioannina,T2_HU_Budapest,T2_IN_TIFR,T2_IT_Bari,T2_IT_Legnaro,T2_IT_Pisa,T2_IT_Rome,T2_KR_KISTI,T2_MY_SIFIR,T2_MY_UPM_BIRUNI,T2_PK_NCP,T2_PL_Swierk,T2_PL_Warsaw,T2_PT_NCG_Lisbon,T2_RU_IHEP,T2_RU_INR,T2_RU_ITEP,T2_RU_JINR,T2_RU_PNPI,T2_RU_SINP,T2_TH_CUNSTDA,T2_TR_METU,T2_TW_NCHC,T2_UA_KIPT,T2_UK_London_IC,T2_UK_SGrid_Bristol,T2_UK_SGrid_RALPP,T2_US_Caltech,T2_US_Florida,T2_US_MIT,T2_US_Nebraska,T2_US_Purdue,T2_US_UCSD,T2_US_Vanderbilt,T2_US_Wisconsin,T3_CH_CERN_CAF,T3_CH_CERN_DOMA,T3_CH_CERN_HelixNebula,T3_CH_CERN_HelixNebula_REHA,T3_CH_CMSAtHome,T3_CH_Volunteer,T3_US_HEPCloud,T3_US_NERSC,T3_US_OSG,T3_US_PSC,T3_US_SDSC"\n')
            
            
            
            frun_condor.write('on_exit_remove = (ExitBySignal == False) && (ExitCode == 0)\n')
            frun_condor.write('max_retries    = 3\n')
            frun_condor.write('+JobFlavour    = "%s"\n'%self.queue)
            frun_condor.write('RequestCpus = %s\n'%self.ncpus)
            
            frun_condor.write('use_x509userproxy = True\n')
            frun_condor.write('x509userproxy = /home/submit/jaeyserm/x509up_u204569\n')
            frun_condor.write('+AccountingGroup = "analysis.jaeyserm"\n')
            
            
            frun_condor.write('queue filename matching files %s\n'%condor_file_str)
            frun_condor.close()
            #
            nbjobsSub=0
            cmdBatch="condor_submit %s"%frunfull_condor
            print (cmdBatch)
            job=ut.SubmitToCondor(cmdBatch,10,"%i/%i"%(nbjobsSub,self.njobs))
            nbjobsSub+=job    
    
        print ('succesfully sent %i  job(s)'%nbjobsSub)

