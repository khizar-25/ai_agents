#!/usr/bin/env python3
import sys, json, os, subprocess, tempfile, logging
import urllib.request, urllib.error

logging.basicConfig(filename=os.path.expanduser("~/ai_agents_mcp.log"), level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

GITHUB_USER   = "sherwinsam07"
GITHUB_REPO   = "ai_agents"
GITHUB_BRANCH = "main"
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN", "")
RAW_BASE = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}"

TOOLS = [
    {
        "name": "run_hadoop_agent",
        "description": (
            "Installs Apache Hadoop 3.4.2 on master and worker nodes. "
            "BEFORE calling this tool you MUST ask the user one by one: "
            "1) master_ip - IP address of master node, "
            "2) master_user - username on master, "
            "3) master_pass - password on master, "
            "4) worker_count - how many worker nodes (0 = single machine), "
            "5) for EACH worker ask: worker_N_ip, worker_N_user, worker_N_pass. "
            "The agent checks SSH to all workers first then installs in parallel. "
            "Trigger: install hadoop, setup hadoop, configure hadoop."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "master_ip":    {"type": "string", "description": "Master node IP e.g. 192.168.1.10 or localhost"},
                "master_user":  {"type": "string", "description": "Username on master node"},
                "master_pass":  {"type": "string", "description": "Password on master node"},
                "worker_count": {"type": "string", "description": "Number of worker nodes, 0 for single machine"},
                "worker_1_ip":   {"type": "string"}, "worker_1_user": {"type": "string"}, "worker_1_pass": {"type": "string"},
                "worker_2_ip":   {"type": "string"}, "worker_2_user": {"type": "string"}, "worker_2_pass": {"type": "string"},
                "worker_3_ip":   {"type": "string"}, "worker_3_user": {"type": "string"}, "worker_3_pass": {"type": "string"}
            },
            "required": ["master_ip", "master_user", "worker_count"]
        }
    },
    {
        "name": "run_spark_agent",
        "description": (
            "Installs Apache Spark on master and worker nodes. "
            "BEFORE calling ask: master_ip, master_user, master_pass, worker_count, worker details. "
            "Trigger: install spark, setup spark."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "master_ip":    {"type": "string"}, "master_user": {"type": "string"}, "master_pass": {"type": "string"},
                "worker_count": {"type": "string"},
                "worker_1_ip":  {"type": "string"}, "worker_1_user": {"type": "string"}, "worker_1_pass": {"type": "string"},
                "worker_2_ip":  {"type": "string"}, "worker_2_user": {"type": "string"}, "worker_2_pass": {"type": "string"},
                "version": {"type": "string", "default": "3.5.0"}
            },
            "required": ["master_ip", "master_user", "worker_count"]
        }
    },
    {
        "name": "run_airflow_agent",
        "description": (
            "Installs Apache Airflow on this machine. "
            "BEFORE calling ask the user: 1) port, 2) username, 3) password. "
            "Trigger: install airflow, reinstall airflow, setup airflow, repair airflow."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "port":     {"type": "string", "description": "Port e.g. 8080"},
                "username": {"type": "string", "description": "Admin username"},
                "password": {"type": "string", "description": "Admin password"},
                "action":   {"type": "string", "default": "reinstall"}
            },
            "required": ["port", "username", "password"]
        }
    }
]

AIRFLOW_FILES = ["main.py","config.py","runner.py","checks.py","installer.py","configurator.py","fixer.py","verifier.py","ai_analyzer.py"]

def fetch_file(path):
    url = f"{RAW_BASE}/{path}"
    req = urllib.request.Request(url)
    if GITHUB_TOKEN:
        req.add_header("Authorization", f"token {GITHUB_TOKEN}")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            c = resp.read().decode("utf-8")
            log.info(f"Fetched {url} ({len(c)} bytes)")
            return c
    except Exception as e:
        log.warning(f"Could not fetch {url}: {e}")
        return None

def fetch_and_run(folder, files, tmp_dir, stdin_input=None, extra_env=None, timeout=900):
    downloaded = []
    for fn in files:
        c = fetch_file(f"{folder}/{fn}")
        if c:
            with open(os.path.join(tmp_dir, fn), "w") as f: f.write(c)
            downloaded.append(fn)
    rules = fetch_file(f"{folder}/rules/error_rules.json")
    if rules:
        os.makedirs(os.path.join(tmp_dir, "rules"), exist_ok=True)
        with open(os.path.join(tmp_dir, "rules", "error_rules.json"), "w") as f: f.write(rules)
        downloaded.append("rules/error_rules.json")
    log.info(f"{folder} downloaded: {downloaded}")
    if "main.py" not in downloaded:
        return f"ERROR: Could not download {folder}/main.py\nURL: {RAW_BASE}/{folder}/main.py"
    fixer = os.path.join(tmp_dir, "fixer.py")
    if os.path.exists(fixer):
        with open(fixer) as f: fc = f.read()
        fc = fc.replace('Path.home() / "airflow_cmd_agent" / "rules" / "error_rules.json"', f'Path("{tmp_dir}") / "rules" / "error_rules.json"')
        with open(fixer, "w") as f: f.write(fc)
    env = os.environ.copy()
    env["PYTHONPATH"] = tmp_dir
    if extra_env: env.update(extra_env)
    try:
        r = subprocess.run([sys.executable, os.path.join(tmp_dir, "main.py")], input=stdin_input, capture_output=True, text=True, timeout=timeout, env=env, cwd=tmp_dir)
        out = r.stdout or ""
        if r.stderr: out += f"\n[stderr]:\n{r.stderr}"
        if r.returncode != 0: out = f"Agent exited {r.returncode}\n{out}"
        return out.strip() or "Agent completed."
    except subprocess.TimeoutExpired: return f"Timed out after {timeout}s."
    except Exception as e: return f"Error: {e}"

def build_worker_env(args, base_env):
    count = int(args.get("worker_count", "0"))
    base_env["AGENT_WORKER_COUNT"] = str(count)
    for i in range(1, count + 1):
        base_env[f"AGENT_WORKER_{i}_IP"]   = args.get(f"worker_{i}_ip", "")
        base_env[f"AGENT_WORKER_{i}_USER"] = args.get(f"worker_{i}_user", base_env.get("AGENT_MASTER_USER",""))
        base_env[f"AGENT_WORKER_{i}_PASS"] = args.get(f"worker_{i}_pass", base_env.get("AGENT_MASTER_PASS",""))
    return base_env

def run_agent(tool_name, args):
    if tool_name == "run_hadoop_agent":
        env = {
            "AGENT_VERSION":     "3.4.2",
            "AGENT_MASTER_IP":   args.get("master_ip",   "localhost"),
            "AGENT_MASTER_USER": args.get("master_user", "vboxuser"),
            "AGENT_MASTER_PASS": args.get("master_pass", ""),
        }
        env = build_worker_env(args, env)
        log.info(f"Hadoop: master={env['AGENT_MASTER_IP']} workers={env['AGENT_WORKER_COUNT']}")
        return fetch_and_run("hadoop_agent", ["main.py"], tempfile.mkdtemp(prefix="hadoop_"), extra_env=env, timeout=900)

    elif tool_name == "run_spark_agent":
        env = {
            "AGENT_VERSION":     args.get("version", "3.5.0"),
            "AGENT_MASTER_IP":   args.get("master_ip",   "localhost"),
            "AGENT_MASTER_USER": args.get("master_user", "vboxuser"),
            "AGENT_MASTER_PASS": args.get("master_pass", ""),
        }
        env = build_worker_env(args, env)
        log.info(f"Spark: master={env['AGENT_MASTER_IP']} workers={env['AGENT_WORKER_COUNT']}")
        return fetch_and_run("spark_agent", ["main.py"], tempfile.mkdtemp(prefix="spark_"), extra_env=env, timeout=600)

    elif tool_name == "run_airflow_agent":
        port     = args.get("port", "8080")
        username = args.get("username", "admin")
        password = args.get("password", "admin")
        action   = args.get("action", "reinstall").lower()
        choice   = {"verify":"1","repair":"2","reinstall":"3"}.get(action, "3")
        stdin    = f"{port}\n{username}\n{password}\n{choice}\n{choice}\n"
        log.info(f"Airflow: port={port} user={username} action={action}")
        return fetch_and_run("airflow_agent", AIRFLOW_FILES, tempfile.mkdtemp(prefix="airflow_"), stdin_input=stdin, timeout=1200)

    return f"Unknown tool: {tool_name}"

def handle(req):
    method = req.get("method",""); rid = req.get("id")
    if method == "initialize":
        return {"jsonrpc":"2.0","id":rid,"result":{"protocolVersion":"2024-11-05","capabilities":{"tools":{}},"serverInfo":{"name":"ai-agents-github-mcp","version":"7.0.0"}}}
    if method == "tools/list":
        return {"jsonrpc":"2.0","id":rid,"result":{"tools":TOOLS}}
    if method == "tools/call":
        p = req.get("params",{}); tn = p.get("name",""); ag = p.get("arguments",{})
        log.info(f"Tool: {tn} args={ag}")
        return {"jsonrpc":"2.0","id":rid,"result":{"content":[{"type":"text","text":run_agent(tn,ag)}],"isError":False}}
    if method == "notifications/initialized": return None
    return {"jsonrpc":"2.0","id":rid,"error":{"code":-32601,"message":f"Method not found: {method}"}}

def main():
    log.info("ai-agents-github-mcp v7.0 started")
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw: continue
        try:
            req = json.loads(raw); resp = handle(req)
            if resp is not None: print(json.dumps(resp), flush=True)
        except json.JSONDecodeError as e:
            print(json.dumps({"jsonrpc":"2.0","id":None,"error":{"code":-32700,"message":f"Parse error: {e}"}}), flush=True)
        except Exception as e:
            log.exception("error")
            print(json.dumps({"jsonrpc":"2.0","id":None,"error":{"code":-32603,"message":str(e)}}), flush=True)

if __name__ == "__main__":
    main()
