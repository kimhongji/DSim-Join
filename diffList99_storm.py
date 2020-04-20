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


f1 = open('/home/user/latency/storm_latency/storm_result.txt1','r')
f2 = open('/home/user/latency/storm_latency/storm_result.txt2','r')
f3 = open('/home/user/latency/storm_latency/storm_result.txt3','r')
f4 = open('/home/user/latency/storm_latency/storm_result.txt4','r')

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
    fList1.append(line.split('|')[1]) 

for line in f2.readlines():
    fList2.append(line.split('|')[1]) 
for line in f3.readlines():
    fList3.append(line.split('|')[1]) 
for line in f4.readlines():
    fList4.append(line.split('|')[1]) 


totalSize = len(fList1)+len(fList2)+len(fList3)+len(fList4)-startIdx
div = len(fList1)+len(fList2)+len(fList3)+len(fList4)-startIdx


for i in range(startIdx,len(fList1)):
    if(i!=len(fList1)-1):
       
        diffVal = int(fList1[i+1])-int(fList1[i])
        
        if(diffVal>0 and diffVal<=150):
            diffList.append(diffVal)
            diffSum += diffVal  
            print("i=",i,fList1[i+1]," / ",fList1[i]," / ", diffVal, " / " ,diffSum)
        else:
            div = div-1
            outlier = outlier + 1

for i in range(startIdx,len(fList2)):
    if(i!=len(fList2)-1):
        
        diffVal = int(fList2[i+1])-int(fList2[i])
        
        if(diffVal>0 and diffVal<=150):
            diffList.append(diffVal)
            diffSum += diffVal  
            print("i=",i,fList2[i+1]," / ",fList2[i]," / ", diffVal, " / " ,diffSum)
        else:
            div = div-1
            outlier = outlier + 1

for i in range(startIdx,len(fList3)):
    if(i!=len(fList3)-1):
    
        diffVal = int(fList3[i+1])-int(fList3[i])
        
        if(diffVal>0 and diffVal<=150):
            diffList.append(diffVal)
            diffSum += diffVal  
            print("i=",i,fList3[i+1]," / ",fList3[i]," / ", diffVal, " / " ,diffSum)
        else:
            div = div-1
            outlier = outlier + 1

for i in range(startIdx,len(fList4)):
    if(i!=len(fList4)-1):
        
        diffVal = int(fList4[i+1])-int(fList4[i])
        
        if(diffVal>0 and diffVal<=150):
            diffList.append(diffVal)
            diffSum += diffVal  
            print("i=",i,fList4[i+1]," / ",fList4[i]," / ", diffVal, " / " ,diffSum)
        else:
            div = div-1
            outlier = outlier + 1

            
        
diffMax = max(diffList)

"""
position, val = probscale.plot_pos(tips['diffList'])
fig, ax = plt.subplots(figsize=(6,3))
ax.plot(position,val,marker='.',linestyle='none',label='Bill amount')
ax.set_xlabel("percentile")
seaborn.despine()
"""

plt.plot(diffList,'o')
plt.show()

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


percentile = np.percentile(diffList,2)

print("diffMax= ",diffMax, "diffAvg=",diffAvg)
print("outNum =",outlier,"totalSize=",totalSize,"outlier=",(float(outlier)/float(totalSize))*100,"%" )
print("98% percentile = ",percentile)





 
    #print(fList)
        
        
#if __name__ == '__main__':
 #   sys.exit(main(sys.argv))
    