# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Python script for throttling datanode process to use two cores.

import multiprocessing
import sys
import subprocess


# Find the process ID for a java process.
def find_process_id(java_class, full_java_class):
  ps_command = "ps aux | grep %s" % java_class 
  proc = subprocess.Popen([ps_command], stdout=subprocess.PIPE, shell=True)
  (out, err) = proc.communicate()
  lines = out.split('\n')
  for line in lines:
    if full_java_class in line:
      return line.split()[1]
  return None


# Find the start and end core for a process.
def get_cores(pid_str):
  taskset_command = "taskset -cp %s" % pid_str
  proc = subprocess.Popen([taskset_command], stdout=subprocess.PIPE, shell=True)
  (out, err) = proc.communicate()
  cores = out.split()[-1]
  if "-" in cores:
    return [int(core) for core in cores.split("-")]
  if "," in cores:
    return [int(core) for core in cores.split(",")]


# Set the start and end core for a process.
def set_cores(pid_str, start_core, end_core):
  taskset_command = "taskset -acp %d-%d %s" % (start_core, end_core, pid_str)
  print "Running %s" % taskset_command
  proc = subprocess.Popen([taskset_command], stdout=subprocess.PIPE, shell=True)
  (out, err) = proc.communicate()


if __name__ == '__main__':
  # Get the process ids.
  terrapin_server_pid = find_process_id("TerrapinServer",
      "com.pinterest.terrapin.server.TerrapinServerMain")
  dn_pid = find_process_id("DataNode",
      "org.apache.hadoop.hdfs.server.datanode.DataNode")
  num_cores = multiprocessing.cpu_count()
  if num_cores <= 2:
    print "Running on a system with too few cores."
    sys.exit(1)
  # Use last two cores for datanode
  terrapin_server_start_core = 0
  terrapin_server_end_core = num_cores - 3
  dn_start_core = terrapin_server_end_core + 1
  dn_end_core = num_cores - 1

  curr_terrapin_server_cores = get_cores(terrapin_server_pid)
  curr_dn_cores = get_cores(dn_pid)
  # If the target cores do not match existing cores, call task set
  # to make the appropriate change.
  if terrapin_server_start_core != curr_terrapin_server_cores[0] or \
     terrapin_server_end_core != curr_terrapin_server_cores[1]:
    set_cores(terrapin_server_pid,
              terrapin_server_start_core,
              terrapin_server_end_core)
  if dn_start_core != curr_dn_cores[0] or \
     dn_end_core != curr_dn_cores[1]:
    set_cores(dn_pid, dn_start_core, dn_end_core)
