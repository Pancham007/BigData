# Databricks notebook source
textfile = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")

# COMMAND ----------

userfriendslist=textfile.map(lambda x:x.split("\t"))
userfriendslist.collect()

# COMMAND ----------

friends=userfriendslist.map(lambda x:x[1].split(","))

# COMMAND ----------

users=userfriendslist.map(lambda x:x[0])

# COMMAND ----------

users_list=users.collect()
friends_list=friends.collect()

# COMMAND ----------

print friends_list

# COMMAND ----------

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

# COMMAND ----------

pairedList=sc.parallelize(pairs)     

# COMMAND ----------

mutualFriends=pairedList.reduceByKey(lambda list1,list2:list(set(list1).intersection(list2)))

# COMMAND ----------

mutualFriendsTest=mutualFriends.filter(lambda x: (
                                x[0]==(0, 4)or x[0]==(20, 22939)or x[0]==(1, 29826)or x[0]==(6222, 19272) or x[0]==(28041, 28056)))

# COMMAND ----------

mutualFriends=mutualFriends.sortByKey().collect()
display(mutualFriends)

# COMMAND ----------

mutualFriendsTest=mutualFriendsTest.sortByKey().collect()
display(mutualFriendsTest)

# COMMAND ----------


