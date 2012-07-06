Redis-migrator
==============
Redis-migrator is a tool to redistribute keys in your redis cluster when its topography has
changed. 

##How it works

Say you are using Redis::Distributed to distribute your writes and reads across different
redis nodes. Redis::Distributed uses consitent hashing algorithm to determine where a command
will go. If you changed configuration of your cluster, for example, changed a hostname or added a new node then routes for some of the keys will change too. If you try to read such a key - you won't get a data from its old node.

Redis-migrator takes a list of nodes for your old cluster and list of nodes for your new cluster 
and determines for which keys routes were changed. Then it moves those keys to new nodes.

##Install  
`gem install redis-migrator`

##Usage  
    require 'redis-migrator'

    # a list of redis-urls for an old cluster
    old_redis_hosts = ["redis://host1.com:6379", "redis://host2.com:6379"]

    # a list of redis-urls for a new cluster
    old_redis_hosts = ["redis://host1.com:6379", "redis://host2.com:6379", "redis://host3.com:6379"]

    migrator = Redis::Migrator.new(old_redis_hosts, new_redis_hosts)
    migrator.run

##Requirements
* ruby 1.9 or jruby (with --1.9 flag)
* redis >=2.4.14 (only on machine where migrator will be running)

##TODO
* Error handling 
