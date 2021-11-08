def parsepandatable(intab):
  print("infile is "+intab)
  f=open(intab,'r')
  done=0
  nquanta={}
  secperstep={}
  sumtime={}
  wallhr={}
  maxmem={}
  totsumsec=0
  totmaxmem=0
  while(done==0):
    lin=f.readline()
    if(len(lin)==0):
      done=1
      continue
    a=lin.strip()
    print("a is"+str(a))
    #b=a.split("|")
    #b=a.split("│")
    b=a.split("│")
    l=len(b)
    print("len:"+str(l)+" a is:"+a)
    if(l>1):
     print("b1 "+b[1])
    if(l>1 and b[1].strip()[0:2]=="20"):
     print("b1 "+b[1])
     upn=b[1].strip()
     pstat=b[2]
     pntasks=b[3]
     pnfiles=b[4]
     pnremain=b[5]
     pnproc=b[6]
     pnfin=b[7]
     pnfail=b[8]
     psubfin=b[9]
     print("statline:",upn,pstat,pntasks,pnfiles,pnremain,pnproc,pnfin,pnfail,psubfin)
     continue
    if(l > 5):
     taskname=b[1].lstrip(" ").rstrip(" ")
     print("len tn:"+str(len(taskname)))
     print("taskname:"+str(taskname))
     if(taskname != "" and taskname != "Campaign"):
      nquanta[taskname]=int(b[2])
      secperstep[taskname]=float(b[4])
      wallclock=b[3]
      hms=wallclock.split(":")
      wallhr[taskname]=int(hms[0])+float(hms[1])/60.0+float(hms[2])/3600.0
      sumtime[taskname]=nquanta[taskname]*secperstep[taskname]/3600.0
      maxmem[taskname]=float(b[6])
      totsumsec = totsumsec + sumtime[taskname]
      totmaxmem = totmaxmem + wallhr[taskname]
      #print(taskname+":",nquanta[taskname],secperstep[taskname],sumtime[taskname],maxmem[taskname],totsumsec,totmaxmem,upn)
  return(totmaxmem,totsumsec,nquanta,secperstep,wallhr,sumtime,maxmem,upn,pstat,pntasks,pnfiles,pnremain,pnproc,pnfin,pnfail,psubfin)
