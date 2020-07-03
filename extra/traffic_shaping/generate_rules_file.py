# writes out traffic shaping rules for us-east* regions to a file

import requests

url = r'https://ip-ranges.amazonaws.com/ip-ranges.json'

data = requests.get(url).json()
INTERFACE = 'eno1'

ip_rules = []

for p in data['prefixes']:
    if 'east' in p['region']:
        rule = f"tc filter add dev {INTERFACE} parent 1: protocol ip prio 16 u32 match ip dst {p['ip_prefix']} flowid 1:1\n"
        ip_rules.append(rule)

with open('ip_rules.sh', 'w') as f:
    for r in ip_rules:
        f.write(r)