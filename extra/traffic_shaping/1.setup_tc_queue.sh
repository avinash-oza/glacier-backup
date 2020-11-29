#! /bin/bash

# taken from https://irulan.net/throttling-linux-network-bandwidth-by-ip-address-and-time-of-day/
# run as ROOT
INTERFACE='enp4s0'

tc qdisc add dev $INTERFACE root handle 1: cbq avpkt 1000 bandwidth 1000mbit

# max at 300KB/s * 8 = 2400kbit
tc class add dev $INTERFACE parent 1: classid 1:1 cbq rate 2400kbit allot 1500 prio 5 bounded isolated

# example cron rules

# From 1-630am maximize upload
#30 1 * * * /sbin/tc class change dev eno1 classid 1:1 cbq rate 100mbit allot 1500 prio 5 bounded isolated
#30 6 * * * /sbin/tc class change dev eno1 classid 1:1 cbq rate 2400kbit allot 1500 prio 5 bounded isolated


