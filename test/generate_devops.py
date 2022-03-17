import random

regions = ["us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-northeast-1", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2", "ap-south-1", "sa-east-1"]
os = ["centos7", "centos8", "RHEL8", "RHEL8.1", "RHEL8.2", "RHEL8.3", "RHEL8.4", "Fedora34", "Fedora33", "Fedora32"]
service_environment = ["production", "staging", "test"]
team = ["CHI", "SF", "LON", "NYC"]

with open("devops100000.txt", "r") as f1, open("devops100000-2.txt", "w") as f2:
	line = f1.readline()
	while line:
		items = line.strip().split(",")
		items[1] = "host_" + str(int(items[1].split("_")[1]) + 100000)
		rid = random.randint(0, len(regions) - 1)
		items[2] = "region=" + regions[rid]
		items[3] = "datacenter=" + regions[rid] + str(chr(ord('a') + random.randint(0, 4)))
		for i in range(4, len(items)):
			sub = items[i].split("=")
			if sub[0] == "rack":
				items[i] = "rack=" + str(random.randint(0, 100))
			elif sub[0] == "os":
				items[i] = "os=" + os[random.randint(0, len(os) - 1)]
			elif sub[0] == "arch":
				if sub[1] == "x86":
					items[i] = "arch=x64"
				else:
					items[i] = "arch=x86"
			elif sub[0] == "team":
				items[i] = "team=" + team[random.randint(0, len(team) - 1)]
			elif sub[0] == "service":
				items[i] = "service=" + str(random.randint(0, 20))
			elif sub[0] == "service_version":
				if sub[1] == "0":
					items[i] = "service_version=1"
				else:
					items[i] = "service_version=0"
			elif sub[0] == "service_environment":
				items[i] = "service_environment=" + service_environment[random.randint(0, len(service_environment) - 1)]
		f2.write(",".join(items) + "\n")
		line = f1.readline()