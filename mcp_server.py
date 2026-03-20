#!/usr/bin/env python3
"""
mcp_server.py - v4.0
Fixes: hadoop 3.4.2, correct file fetching, master/worker nodes, all input() prompts
"""

import sys, json, os, subprocess, tempfile, logging
import urllib.request, urllib.error

logging.basicConfig(
    filename=os.path.expanduser("~/ai_agents_mcp.log"),
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
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
            "Fetches and runs hadoop_agent from github.com/sherwinsam07/ai_agents "
            "to install Apache Hadoop 3.4.2 on this machine. "
            "Ask the user for master hostname and number of worker nodes before calling. "
            "Trigger when user says: install hadoop, setup hadoop, start hadoop, configure hadoop."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "master_host":  {"type": "string", "description": "Master node hostname or IP", "default": "localhost"},
                "worker_nodes": {"type": "string", "description": "Number of worker nodes", "default": "1"},
                "mode":         {"type": "string", "description": "pseudo or cluster", "default": "pseudo"}
            }
        }
    },
    {
        "name": "run_spark_agent",
        "description": (
            "Fetches and runs spark_agent from github.com/sherwinsam07/ai_agents "
            "to install Apache Spark on this machine. "
            "Trigger when user says: install spark, setup spark."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "version":     {"type": "string", "default": "3.5.0"},
                "master_host": {"type": "string", "default": "localhost"}
            }
        }
    },
    {
        "name": "run_airflow_agent",
        "description": (
            "Fetches and runs airflow_agent from github.com/sherwinsam07/ai_agents "
            "to install or reinstall Apache Airflow. "
            "Trigger when user says: install airflow, reinstall airflow, setup airflow, repair airflow."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "action":   {"type": "string", "description": "verify, repair, or reinstall", "default": "reinstall"},
                "port":     {"type": "string", "default": "8080"},
                "username": {"type": "string", "default": "admin"},
                "password": {"type": "string", "default": "admin"}
            }
        }
    }
]

AIRFLOW_FILES = [
    "main.py", "config.py", "runner.py", "checks.py",
    "installer.py", "configurator.py", "fixer.py",
    "verifier.py", "ai_analyzer.py"
]
HADOOP_FILES = ["main.py"]
SPARK_FILES  = ["main.py"]


def fetch_file(path):
    url = f"{RAW_BASE}/{path}"
    req = urllib.request.Request(url)
    if GITHUB_TOKEN:
        req.add_header("Authorization", f"token {GITHUB_TOKEN}")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read().decode("utf-8")
            log.info(f"Fetched {url} ({len(content)} bytes)")
            return content
    except Exception as e:
        log.warning(f"Could not fetch {url}: {e}")
        return None


def fetch_agent_files(agent_folder, file_list, tmp_dir):
    downloaded = []
    for filename in file_list:
        content = fetch_file(f"{agent_folder}/{filename}")
        if content:
            with open(os.path.join(tmp_dir, filename), "w") as f:
                f.write(content)
            downloaded.append(filename)
    rules = fetch_file(f"{agent_folder}/rules/error_rules.json")
    if rules:
        os.makedirs(os.path.join(tmp_dir, "rules"), exist_ok=True)
        with open(os.path.join(tmp_dir, "rules", "error_rules.json"), "w") as f:
            f.write(rules)
        downloaded.append("rules/error_rules.json")
    return "main.py" in downloaded, downloaded


def fix_airflow_paths(tmp_dir):
    fixer_path = os.path.join(tmp_dir, "fixer.py")
    if os.path.exists(fixer_path):
        with open(fixer_path, "r") as f:
            content = f.read()
        content = content.replace(
            'Path.home() / "airflow_cmd_agent" / "rules" / "error_rules.json"',
            f'Path("{tmp_dir}") / "rules" / "error_rules.json"'
        )
        with open(fixer_path, "w") as f:
            f.write(content)


def run_script(tmp_dir, stdin_input=None, timeout=1200, extra_env=None):
    main_path = os.path.join(tmp_dir, "main.py")
    env = os.environ.copy()
    env["PYTHONPATH"] = tmp_dir
    if extra_env:
        env.update(extra_env)
    try:
        result = subprocess.run(
            [sys.executable, main_path],
            input=stdin_input,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            cwd=tmp_dir
        )
        output = result.stdout or ""
        if result.stderr:
            output += f"\n[stderr]:\n{result.stderr}"
        if result.returncode != 0:
            output = f"Agent exited with code {result.returncode}\n{output}"
        return output.strip() or "Agent completed with no output."
    except subprocess.TimeoutExpired:
        return "Agent timed out."
    except Exception as e:
        return f"Error running agent: {e}"


def run_agent(tool_name, args):

    if tool_name == "run_hadoop_agent":
        master_host  = args.get("master_host", "localhost")
        worker_nodes = args.get("worker_nodes", "1")
        mode         = args.get("mode", "pseudo")
        tmp_dir = tempfile.mkdtemp(prefix="hadoop_agent_")
        ok, downloaded = fetch_agent_files("hadoop_agent", HADOOP_FILES, tmp_dir)
        log.info(f"Hadoop downloaded: {downloaded}")
        if not ok:
            return (
                f"Could not download hadoop_agent/main.py from GitHub.\n"
                f"Check: github.com/{GITHUB_USER}/{GITHUB_REPO}/blob/{GITHUB_BRANCH}/hadoop_agent/main.py\n"
                f"Downloaded: {downloaded}"
            )
        extra_env = {
            "AGENT_VERSION":      "3.4.2",
            "AGENT_MASTER_HOST":  master_host,
            "AGENT_WORKER_NODES": worker_nodes,
            "AGENT_MODE":         mode,
        }
        stdin_input = f"{master_host}\n{worker_nodes}\n{mode}\n"
        log.info(f"Running hadoop: master={master_host} workers={worker_nodes} mode={mode}")
        return run_script(tmp_dir, stdin_input=stdin_input, timeout=900, extra_env=extra_env)

    elif tool_name == "run_spark_agent":
        version     = args.get("version", "3.5.0")
        master_host = args.get("master_host", "localhost")
        tmp_dir = tempfile.mkdtemp(prefix="spark_agent_")
        ok, downloaded = fetch_agent_files("spark_agent", SPARK_FILES, tmp_dir)
        log.info(f"Spark downloaded: {downloaded}")
        if not ok:
            return f"Could not download spark_agent/main.py.\nDownloaded: {downloaded}"
        extra_env = {
            "AGENT_VERSION":     version,
            "AGENT_MASTER_HOST": master_host,
        }
        stdin_input = f"{master_host}\n1\n"
        return run_script(tmp_dir, stdin_input=stdin_input, timeout=600, extra_env=extra_env)

    elif tool_name == "run_airflow_agent":
        port     = args.get("port", "8080")
        username = args.get("username", "admin")
        password = args.get("password", "admin")
        action   = args.get("action", "reinstall").lower()
        choice   = {"verify": "1", "repair": "2", "reinstall": "3"}.get(action, "3")
        tmp_dir = tempfile.mkdtemp(prefix="airflow_agent_")
        ok, downloaded = fetch_agent_files("airflow_agent", AIRFLOW_FILES, tmp_dir)
        log.info(f"Airflow downloaded: {downloaded}")
        if not ok:
            return f"Could not download airflow_agent/main.py.\nDownloaded: {downloaded}"
        fix_airflow_paths(tmp_dir)
        stdin_input = f"{port}\n{username}\n{password}\n{choice}\n{choice}\n"
        log.info(f"Running airflow: action={action} choice={choice} port={port} user={username}")
        return run_script(tmp_dir, stdin_input=stdin_input, timeout=1200)

    return f"Unknown tool: {tool_name}"


def handle(req):
    method = req.get("method", "")
    rid    = req.get("id")
    if method == "initialize":
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "ai-agents-github-mcp", "version": "4.0.0"}
        }}
    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": rid, "result": {"tools": TOOLS}}
    if method == "tools/call":
        params    = req.get("params", {})
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        log.info(f"Tool call: {tool_name}  args={arguments}")
        output = run_agent(tool_name, arguments)
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "content": [{"type": "text", "text": output}],
            "isError": False
        }}
    if method == "notifications/initialized":
        return None
    return {"jsonrpc": "2.0", "id": rid,
            "error": {"code": -32601, "message": f"Method not found: {method}"}}


def main():
    log.info("ai-agents-github-mcp v4.0 started")
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            req  = json.loads(raw)
            resp = handle(req)
            if resp is not None:
                print(json.dumps(resp), flush=True)
        except json.JSONDecodeError as e:
            print(json.dumps({"jsonrpc": "2.0", "id": None,
                "error": {"code": -32700, "message": f"Parse error: {e}"}}), flush=True)
        except Exception as e:
            log.exception("Unhandled error")
            print(json.dumps({"jsonrpc": "2.0", "id": None,
                "error": {"code": -32603, "message": str(e)}}), flush=True)

if __name__ == "__main__":
    main()
