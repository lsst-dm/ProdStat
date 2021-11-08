def parsepandatable(intab):
  print("infile is "+intab)
  f=open(intab,'r')
  done=0
  nquanta={}
  startdate={}
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
      startdate[taskname]=b[3]
      secperstep[taskname]=float(b[5])
      wallclock=b[4]
      hms=wallclock.split(":")
      daysplit=hms[0].split("day")
      print("day:",daysplit,len(daysplit))
      if(len(daysplit)>1):
       daysplit2=hms[0].split("days,")
       if(len(daysplit2)>1):
        wallhr[taskname]=int(daysplit2[0])*24+int(daysplit2[1])+float(hms[1])/60.0+float(hms[2])/3600.0
       else:
        daysplit=hms[0].split("day,")
        wallhr[taskname]=int(daysplit[0])*24+int(daysplit[1])+float(hms[1])/60.0+float(hms[2])/3600.0
      else:
       wallhr[taskname]=int(hms[0])+float(hms[1])/60.0+float(hms[2])/3600.0
      sumtime[taskname]=nquanta[taskname]*secperstep[taskname]/3600.0
      maxmem[taskname]=float(b[7])
      totsumsec = totsumsec + sumtime[taskname]
      totmaxmem = totmaxmem + wallhr[taskname]
      #print(taskname+":",nquanta[taskname],secperstep[taskname],sumtime[taskname],maxmem[taskname],totsumsec,totmaxmem,upn)
  return(totmaxmem,totsumsec,nquanta,startdate,secperstep,wallhr,sumtime,maxmem,upn,pstat,pntasks,pnfiles,pnremain,pnproc,pnfin,pnfail,psubfin)
