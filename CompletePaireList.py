# Databricks notebook source
# Databricks notebook source
textfile = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")

userfriendslist=textfile.map(lambda x:x.split("\t"))
userfriendslist.collect()

friends=userfriendslist.map(lambda x:x[1].split(","))

users=userfriendslist.map(lambda x:x[0])

users_list=users.collect()
friends_list=friends.collect()

pairs=[]
if friends_list:
  for friend in friends_list:
    val=friend
    j=users_list.pop(0)
    for i in val:
      if(i!=''):
        if(int(i)>int(j)):
           pairs.append(((int(j),int(i)),val))
        else:
           pairs.append(((int(i),int(j)),val))

pairedList=sc.parallelize(pairs)     

mutualFriends=pairedList.reduceByKey(lambda list1,list2:list(set(list1).intersection(list2)))

mutualFriends=mutualFriends.sortByKey()


# COMMAND ----------

count=mutualFriends.mapValues(lambda x: len(x))

# COMMAND ----------

topten=count.map(lambda y: (y[1],y[0])).sortByKey(ascending=False)

# COMMAND ----------

toptenvalue=topten.collect()[0:10]

# COMMAND ----------

user=sc.textFile("/FileStore/tables/userdata.txt")
userDetails=user.map(lambda x:x.split(","))

# COMMAND ----------

userDetailsPairs=userDetails.map(lambda x: (int(x[0]),x[1:]))

# COMMAND ----------

value=userDetailsPairs.lookup(pair[0])

# COMMAND ----------

print value

# COMMAND ----------

print value[0][0]

# COMMAND ----------

detailedtopten=[]
for item in toptenvalue:
  mutualcount=item[0]
  pair=item[1]
  friendA=userDetailsPairs.lookup(pair[0])
  friendB=userDetailsPairs.lookup(pair[1])
  nameA=str(friendA[0][0])
  nameB=str(friendB[0][0])
  lastnameA=str(friendA[0][1])
  lastnameB=str(friendB[0][1])
  addressA=str(friendA[0][2])+" "+str(friendA[0][3])+" "+str(friendA[0][4])+" "+str(friendA[0][5])+" "+str(friendA[0][6])
  addressB=str(friendB[0][2])+" "+str(friendB[0][3])+" "+str(friendB[0][4])+" "+str(friendB[0][5])+" "+str(friendB[0][6])
  detailedtopten.append(str(mutualcount) +' --FRIEND1-- '+nameA +'    '+lastnameA +'    '+addressA +' --FRIEND1-- '+ nameB +'    '+lastnameB +'    '+addressB)

# COMMAND ----------

  sc.parallelize(detailedtopten).collect()
