#!/usr/bin/env python3
import os, subprocess, sys, urllib.request, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

HADOOP_VERSION = os.environ.get("AGENT_VERSION",     "3.4.2")
MASTER_IP      = os.environ.get("AGENT_MASTER_IP",   "localhost")
MASTER_USER    = os.environ.get("AGENT_MASTER_USER", "vboxuser")
MASTER_PASS    = os.environ.get("AGENT_MASTER_PASS", "")
WORKER_COUNT   = int(os.environ.get("AGENT_WORKER_COUNT", "0"))
HADOOP_SBIN    = "/opt/hadoop/sbin"
HADOOP_BIN     = "/opt/hadoop/bin"
HDFS_BIN       = "/opt/hadoop/bin/hdfs"
INSTALL_DIR    = "/opt"

WORKERS = []
for i in range(1, WORKER_COUNT + 1):
    ip   = os.environ.get(f"AGENT_WORKER_{i}_IP",   "")
    user = os.environ.get(f"AGENT_WORKER_{i}_USER", MASTER_USER)
    pw   = os.environ.get(f"AGENT_WORKER_{i}_PASS", MASTER_PASS)
    if ip:
        WORKERS.append({"ip": ip, "user": user, "pass": pw})

def run(cmd):
    print(f">>> {cmd[:100]}")
    r = subprocess.run(cmd, shell=True, executable="/bin/bash", text=True, capture_output=True)
    if r.stdout.strip(): print(r.stdout.strip()[:400])
    if r.stderr.strip(): print(r.stderr.strip()[:200])
    return r

def run_ssh(host, user, cmd):
    return run(f'ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 {user}@{host} "{cmd}"')

def step(n, msg):
    print(f"\n{'='*55}\n  Step {n}: {msg}\n{'='*55}")

def get_java_home():
    r = run("dirname $(dirname $(readlink -f $(which java)))")
    jh = r.stdout.strip()
    if jh and os.path.exists(jh): return jh
    for p in ["/usr/lib/jvm/java-11-openjdk-amd64","/usr/lib/jvm/java-11-openjdk","/usr/lib/jvm/java-8-openjdk-amd64"]:
        if os.path.exists(p): return p
    return "/usr/lib/jvm/java-11-openjdk-amd64"

def check_java():
    step(1, "Checking Java")
    r = run("java -version")
    if r.returncode != 0:
        run("sudo apt-get update -qq && sudo apt-get install -y openjdk-11-jdk")
    print("Java ready.")

def check_ssh_workers():
    if not WORKERS:
        print("No workers - single machine mode.")
        return
    step(2, "Checking SSH to all worker nodes")
    failed = []
    for w in WORKERS:
        r = run_ssh(w["ip"], w["user"], "echo SSH_OK")
        if "SSH_OK" in (r.stdout or ""):
            print(f"  OK    {w['ip']} ({w['user']})")
        else:
            print(f"  FAIL  {w['ip']} - passwordless SSH not set up")
            print(f"  Fix:  ssh-copy-id {w['user']}@{w['ip']}")
            failed.append(w["ip"])
    if failed:
        print(f"\nERROR: Cannot reach workers: {failed}")
        print("Set up passwordless SSH first, then retry.")
        sys.exit(1)
    print("All workers reachable via SSH.")

def download_hadoop():
    step(3, f"Downloading Hadoop {HADOOP_VERSION}")
    tarball = f"/tmp/hadoop-{HADOOP_VERSION}.tar.gz"
    if os.path.exists(tarball):
        print(f"Already downloaded: {tarball}")
        return tarball
    mirrors = [
        f"https://downloads.apache.org/hadoop/common/hadoop-{HADOOP_VERSION}/hadoop-{HADOOP_VERSION}.tar.gz",
        f"https://archive.apache.org/dist/hadoop/common/hadoop-{HADOOP_VERSION}/hadoop-{HADOOP_VERSION}.tar.gz",
    ]
    for url in mirrors:
        print(f"Trying: {url}")
        try:
            urllib.request.urlretrieve(url, tarball)
            print(f"Downloaded: {tarball}")
            return tarball
        except Exception as e:
            print(f"Failed: {e}")
    print("ERROR: Download failed from all mirrors.")
    sys.exit(1)

def install_on_master(tarball):
    step(4, "Installing Hadoop on master")
    target = f"{INSTALL_DIR}/hadoop-{HADOOP_VERSION}"
    if os.path.exists(target):
        run(f"sudo rm -rf {target}")
    run(f"sudo tar -xzf {tarball} -C {INSTALL_DIR}")
    run(f"sudo ln -sfn {target} /opt/hadoop")
    run(f"sudo chown -R {MASTER_USER}:{MASTER_USER} {target}")
    run(f"sudo chown -R {MASTER_USER}:{MASTER_USER} /opt/hadoop")
    print(f"Master install done: /opt/hadoop -> {target}")

def install_on_worker(w, tarball):
    ip, user = w["ip"], w["user"]
    print(f"  Installing on worker {ip}...")
    run(f"scp -o StrictHostKeyChecking=no {tarball} {user}@{ip}:{tarball}")
    run_ssh(ip, user, f"sudo tar -xzf {tarball} -C {INSTALL_DIR}")
    run_ssh(ip, user, f"sudo ln -sfn {INSTALL_DIR}/hadoop-{HADOOP_VERSION} /opt/hadoop")
    print(f"  Worker {ip} done.")
    return ip

def install_workers_parallel(tarball):
    if not WORKERS: return
    step(5, f"Installing on {len(WORKERS)} worker(s) in parallel")
    with ThreadPoolExecutor(max_workers=len(WORKERS)) as ex:
        futures = {ex.submit(install_on_worker, w, tarball): w for w in WORKERS}
        for fut in as_completed(futures):
            try: print(f"  Completed: {fut.result()}")
            except Exception as e: print(f"  Error: {e}")

def configure_env(java_home):
    step(6, "Configuring environment variables in ~/.bashrc")
    bashrc = Path.home() / ".bashrc"
    content = bashrc.read_text() if bashrc.exists() else ""
    if "=== Hadoop" in content:
        lines = content.split("\n")
        new_lines, skip = [], False
        for line in lines:
            if "=== Hadoop" in line and "hadoop_agent" in line: skip = True
            if skip and "=== End Hadoop ===" in line: skip = False; continue
            if not skip: new_lines.append(line)
        content = "\n".join(new_lines)
    block = f"""
# === Hadoop {HADOOP_VERSION} - hadoop_agent ===
export JAVA_HOME={java_home}
export HADOOP_HOME=/opt/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
# === End Hadoop ===
"""
    bashrc.write_text(content + block)
    hadoop_env = "/opt/hadoop/etc/hadoop/hadoop-env.sh"
    if os.path.exists(hadoop_env):
        run(f"sudo sed -i '/^export JAVA_HOME/d' {hadoop_env}")
        run(f"echo 'export JAVA_HOME={java_home}' | sudo tee -a {hadoop_env}")
    print("Environment configured.")

def configure_hadoop():
    step(7, "Writing Hadoop config files using sudo tee")
    conf = "/opt/hadoop/etc/hadoop"
    nn = Path.home() / "hadoop_data/namenode"
    dn = Path.home() / "hadoop_data/datanode"
    nn.mkdir(parents=True, exist_ok=True)
    dn.mkdir(parents=True, exist_ok=True)
    replication = max(1, min(len(WORKERS) + 1, 3))

    # Use sudo tee to write files — works regardless of directory ownership
    import subprocess as sp

    core = f"""<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://{MASTER_IP}:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>/opt/hadoop/tmp</value></property>
</configuration>"""
    sp.run(f"echo '{core}' | sudo tee {conf}/core-site.xml > /dev/null", shell=True)

    hdfs = f"""<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>dfs.replication</name><value>{replication}</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://{nn}</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://{dn}</value></property>
</configuration>"""
    sp.run(f"echo '{hdfs}' | sudo tee {conf}/hdfs-site.xml > /dev/null", shell=True)

    mapred = """<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
</configuration>"""
    sp.run(f"echo '{mapred}' | sudo tee {conf}/mapred-site.xml > /dev/null", shell=True)

    yarn = f"""<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.resourcemanager.hostname</name><value>{MASTER_IP}</value></property>
</configuration>"""
    sp.run(f"echo '{yarn}' | sudo tee {conf}/yarn-site.xml > /dev/null", shell=True)

    workers_str = "\n".join([w["ip"] for w in WORKERS]) if WORKERS else "localhost"
    sp.run(f"echo '{workers_str}' | sudo tee {conf}/workers > /dev/null", shell=True)

    sp.run("sudo mkdir -p /opt/hadoop/tmp && sudo chmod 777 /opt/hadoop/tmp", shell=True)
    sp.run(f"sudo chown -R {MASTER_USER}:{MASTER_USER} /opt/hadoop-{HADOOP_VERSION} /opt/hadoop", shell=True)
    print("Config files written successfully.")
def setup_ssh():
    step(8, "Setting up passwordless SSH on master")
    run("sudo apt-get install -y -qq openssh-server openssh-client")
    ssh_dir = Path.home() / ".ssh"
    ssh_dir.mkdir(exist_ok=True, mode=0o700)
    key = ssh_dir / "id_rsa"
    if not key.exists():
        run(f"ssh-keygen -t rsa -P '' -f {key} -q")
    pub = (ssh_dir / "id_rsa.pub").read_text().strip()
    auth = ssh_dir / "authorized_keys"
    existing = auth.read_text() if auth.exists() else ""
    if pub not in existing:
        with open(auth, "a") as f: f.write(pub + "\n")
    auth.chmod(0o600)
    run("sudo service ssh start || sudo systemctl start ssh")
    r = run("ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 localhost echo SSH_OK")
    print("SSH localhost OK." if "SSH_OK" in (r.stdout or "") else "WARNING: SSH failed.")

def format_namenode():
    step(9, "Formatting HDFS namenode")
    r = run(f"sudo {HDFS_BIN} namenode -format -force")
    print("Formatted." if r.returncode == 0 else "WARNING: Format issues.")

def start_hadoop():
    step(10, "Starting Hadoop daemons - using full paths")
    run(f"sudo {HADOOP_SBIN}/start-dfs.sh")
    run(f"sudo {HADOOP_SBIN}/start-yarn.sh")
    time.sleep(6)

def verify():
    step(11, "Verifying all daemons running")
    java_home = get_java_home()
    r = run(f"{java_home}/bin/jps")
    if r.returncode != 0: r = run("jps")
    running = r.stdout
    all_ok = True
    for p in ["NameNode", "DataNode", "ResourceManager", "NodeManager"]:
        if p in running: print(f"  OK      {p}")
        else: print(f"  MISSING {p}"); all_ok = False
    run(f"{HADOOP_BIN}/hadoop version")
    return all_ok

def main():
    print("\n" + "="*55)
    print(f"  HADOOP AGENT v3 - Hadoop {HADOOP_VERSION}")
    print(f"  Master : {MASTER_IP} ({MASTER_USER})")
    print(f"  Workers: {WORKER_COUNT}")
    print("="*55)
    check_java()
    check_ssh_workers()
    java_home = get_java_home()
    tarball = download_hadoop()
    install_on_master(tarball)
    install_workers_parallel(tarball)
    configure_env(java_home)
    configure_hadoop()
    setup_ssh()
    format_namenode()
    start_hadoop()
    ok = verify()
    print("\n" + "="*55)
    if ok:
        print(f"  SUCCESS: Hadoop {HADOOP_VERSION} running!")
        print(f"  HDFS: http://{MASTER_IP}:9870")
        print(f"  YARN: http://{MASTER_IP}:8088")
        print(f"  Commands:")
        print(f"    source ~/.bashrc")
        print(f"    hadoop version")
        print(f"    hdfs dfs -ls /")
    else:
        print(f"  Installed but some daemons missing.")
        print(f"  Check: /opt/hadoop/logs/")
        print(f"  Try: sudo {HADOOP_SBIN}/start-dfs.sh")
    print("="*55 + "\n")

if __name__ == "__main__":
    main()
