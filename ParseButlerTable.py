def parsebutlertable(intab):
  print("infile is "+intab)
  f=open(intab,'r')
  done=0
  nquanta={}
  startdate={}
  secperstep={}
  sumtime={}
  maxmem={}
  totsumsec=0
  totmaxmem=0
  while(done==0):
    lin=f.readline()
    if(len(lin)==0):
      done=1
      continue
    a=lin.strip()
    b=a.split()
    print("b:"+str(b))
    if(len(b) > 0 and b[0]=="with"):
     upn=b[2][2:-2]
     print("uid:"+upn)
     continue
    print("a is"+str(a))
    #b=a.split("|")
    b=a.split("│")
    l=len(b)
    print("len:"+str(l))
    if(l > 5):
     taskname=b[1].lstrip(" ").rstrip(" ")
     print("len tn:"+str(len(taskname)))
     print("taskname:"+str(taskname))
     if(taskname != ""):
      nquanta[taskname]=int(b[2])
      startdate[taskname]=b[3]
      secperstep[taskname]=float(b[4])
      sumtime[taskname]=nquanta[taskname]*secperstep[taskname]/3600.0
      maxmem[taskname]=float(b[6])
      totsumsec = totsumsec + sumtime[taskname]
      if (maxmem[taskname] > totmaxmem and taskname!= "Campaign"):
       totmaxmem = maxmem[taskname]
       print("bumping maxmem to "+str(totmaxmem))
      #print(taskname+":",nquanta[taskname],secperstep[taskname],sumtime[taskname],maxmem[taskname],totsumsec,totmaxmem,upn)
  return(totmaxmem,totsumsec,nquanta,startdate,secperstep,sumtime,maxmem,upn)
