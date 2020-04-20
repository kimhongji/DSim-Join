# -*- coding: utf-8 -*-
"""
Spyder Editor

This temporary script file is located here:
/home/user/.spyder2/.temp.py
"""
import sys
import numpy as np
#import pandas as pd
import matplotlib.pyplot as plt
import probscale
import seaborn

"""
f1 = open('/home/user/latency/storm_latency/storm_result.txt1','r')
f2 = open('/home/user/latency/storm_latency/storm_result.txt2','r')
f3 = open('/home/user/latency/storm_latency/storm_result.txt3','r')
f4 = open('/home/user/latency/storm_latency/storm_result.txt4','r')
"""
"""
f1 = open('/home/user/latency/esper_latency/tpch10000/esper_result.txt1','r')
f2 = open('/home/user/latency/esper_latency/tpch10000/esper_result.txt2','r')
f3 = open('/home/user/latency/esper_latency/tpch10000/esper_result.txt3','r')
f4 = open('/home/user/latency/esper_latency/tpch10000/esper_result.txt4','r')
"""

f1 = open('/home/user/latency/esper_latency/weather/sf1/sed_231','r')
f2 = open('/home/user/latency/esper_latency/weather/sf1/sed_232','r')
f3 = open('/home/user/latency/esper_latency/weather/sf1/sed_233','r')
f4 = open('/home/user/latency/esper_latency/weather/sf1/sed_234','r')


fList1=[]
fList2=[]
fList3=[]
fList4=[]
diffList=[]

diffSum = 0
diffAvg = 0
diffMax = 0
outlier=0
startIdx = 0

#fList = f.readlines()

#print("a") 
for line in f1.readlines():
    if(line.split(' ')[1]=="ms\n"):
        fList1.append(line.split(' ')[0]) 

for line in f2.readlines():
    if(line.split(' ')[1]=="ms\n"):
        fList2.append(line.split(' ')[0]) 
for line in f3.readlines():
    if(line.split(' ')[1]=="ms\n"):
        fList3.append(line.split(' ')[0]) 
for line in f4.readlines():
    if(line.split(' ')[1]=="ms\n"):
        fList4.append(line.split(' ')[0]) 


#div=len(fList1)
totalSize = len(fList1)+len(fList2)+len(fList3)+len(fList4)-startIdx
div = len(fList1)+len(fList2)+len(fList3)+len(fList4)-startIdx

boundary = 35
for i in range(startIdx,len(fList1)):
    val=int(fList1[i])
    if(val!=0 and val<=boundary):
        diffSum +=val
        diffList.append(val)
        print("i=",i," val=",val, " diffSum=",diffSum)
    else:
        div = div-1
        outlier=outlier+1
        
for i in range(startIdx,len(fList2)):
    val=int(fList2[i])
    if(val!=0 and val<=boundary):
        diffSum +=val
        diffList.append(val)
        print("i=",i," val=",val, " diffSum=",diffSum)
    else:
        div = div-1
        outlier=outlier+1
        
for i in range(startIdx,len(fList3)):
    val=int(fList3[i])
    if(val!=0 and val<=boundary):
        diffSum +=val
        diffList.append(val)
        print("i=",i," val=",val, " diffSum=",diffSum)
    else:
        div = div-1
        outlier=outlier+1
        
for i in range(startIdx,len(fList4)):
    val=int(fList4[i])
    if(val!=0 and val<=boundary):
        diffSum +=val
        diffList.append(val)
        print("i=",i," val=",val, " diffSum=",diffSum)
    else:
        div = div-1
        outlier=outlier+1
    
             
        
diffMax = max(diffList)



plt.plot(diffList,'o')
plt.show()

"""
position, latency = probscale.plot_pos(diffList)
fig, ax = plt.subplots(figsize=(10, 3))
ax.plot(position, latency, marker='.', linestyle='none')
ax.set_xlabel('Percentile')
ax.set_ylabel('latency')
#ax.set_yscale('log')
#ax.set_ylim(bottom=1, top=100)
seaborn.despine()
"""

fig, (ax1, ax2) = plt.subplots(figsize=(10, 6), ncols=2, sharex=True)
markers = dict(marker='.', linestyle='none', label='Bill Amount')

fig = probscale.probplot(diffList, ax=ax1, plottype='pp', probax='y',
                          problabel='Percentiles',
                         datalabel='Latency', scatter_kws=markers)

fig = probscale.probplot(diffList, ax=ax2, plottype='qq', probax='y',
                          problabel='Standard Normal Quantiles',
                         datalabel='Latency', scatter_kws=markers)

#ax1.set_xlim(left=1, right=100)
fig.tight_layout()
seaborn.despine()


"""
diffList.sort(reverse=True)

print()
for i in range(0,1000):
    print("max = ",diffList[i])
""" 

diffAvg = float(diffSum) / float(div)

print("diffMax= ",diffMax, "diffAvg=",diffAvg, " outlier=",(float(outlier)/float(totalSize))*100)
print("outNum =",outlier,"totalSize=",totalSize)




 
    #print(fList)
        
        
#if __name__ == '__main__':
 #   sys.exit(main(sys.argv))
    