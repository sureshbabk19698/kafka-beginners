## Learning Journey

I am currently on a learning journey, and a significant part of my education is derived from an Udemy Course -- **Apache Kafka Series - Learn Apache Kafka for Beginners v3**, and it has been a valuable resource in expanding my knowledge in Apache Kafka. I highly recommend checking it out if you're interested in the same.

Feel free to explore the <a href="https://www.udemy.com/course/apache-kafka/?utm_source=adwords&utm_medium=udemyads&utm_campaign=DSA_Catchall_la.EN_cc.INDIA&utm_content=deal4584&utm_term=_._ag_82569850245_._ad_533220805574_._kw__._de_c_._dm__._pl__._ti_aud-2268488108799%3Adsa-437115340933_._li_1007809_._pd__._&matchtype=&gad_source=1&gclid=CjwKCAiA2pyuBhBKEiwApLaIO2YJDJjGbCvAZQNVV_lgOFPQ0G3qy2oNqGb92AxhXO7Y_SpPtjzr3hoCzaAQAvD_BwE)https://www.udemy.com/course/apache-kafka/?utm_source=adwords&utm_medium=udemyads&utm_campaign=DSA_Catchall_la.EN_cc.INDIA&utm_content=deal4584&utm_term=_._ag_82569850245_._ad_533220805574_._kw__._de_c_._dm__._pl__._ti_aud-2268488108799%3Adsa-437115340933_._li_1007809_._pd__._&matchtype=&gad_source=1&gclid=CjwKCAiA2pyuBhBKEiwApLaIO2YJDJjGbCvAZQNVV_lgOFPQ0G3qy2oNqGb92AxhXO7Y_SpPtjzr3hoCzaAQAvD_BwE">course</a>

-----------------------------------------------------------------------------
## Producer:
    # Can have number of partitions and replication factor as desired

    # Send data to topic - without keys
        # Without keys - will be assigned to different partitions
        # For datas send without key, the partition will be assigned based on **partitioner.class** property.
          Eg: RoundRobinPartitioner(self explained), StickyPartitioner (batch of records goes to single partition)

    # Send data to topic - with keys
        # same key will always be assigned to same partition using Murmur2 algorithm or using CustomPartitioner(user created logic)
    
-----------------------------------------------------------------------------

## Consumer: 
    # Can have number of consumer groups and consumers as desired.
    # Consumers are assigned partitions based on RangeAssignor, CooperativeStickyAssignor, RoundRobinAssignor, etc,
      can be dynamically configured via **partition.assignment.strategy** property.

    

