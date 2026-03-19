#!/usr/bin/env python3
"""
mcp_server.py - v2.0
Fixed: downloads ALL agent files + passes automated input to main.py
"""

import sys, json, os, subprocess, tempfile, stat, logging
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
        "description": "Fetches and runs hadoop_agent from github.com/sherwinsam07/ai_agents to install Apache Hadoop. Trigger when user says: install hadoop, setup hadoop.",
        "inputSchema": {"type": "object", "properties": {"version": {"type": "string", "default": "3.3.6"}}}
    },
    {
        "name": "run_spark_agent",
        "description": "Fetches and runs spark_agent from github.com/sherwinsam07/ai_agents to install Apache Spark. Trigger when user says: install spark, setup spark.",
        "inputSchema": {"type": "object", "properties": {"version": {"type": "string", "default": "3.5.0"}}}
    },
    {
        "name": "run_airflow_agent",
        "description": "Fetches and runs airflow_agent from github.com/sherwinsam07/ai_agents to install Apache Airflow. Trigger when user says: install airflow, setup airflow.",
        "inputSchema": {
            "type": "object",
            "properties": {
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
GENERIC_FILES = ["main.py", "agent.py", "install.py", "run.py", "config.py", "runner.py"]


def fetch_file(path):
    url = f"{RAW_BASE}/{path}"
    req = urllib.request.Request(url)
    if GITHUB_TOKEN:
        req.add_header("Authorization", f"token {GITHUB_TOKEN}")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        log.warning(f"Could not fetch {url}: {e}")
        return None


def fetch_folder(agent_folder, tmp_dir):
    files = AIRFLOW_FILES if "airflow" in agent_folder else GENERIC_FILES
    downloaded = []
    for filename in files:
        content = fetch_file(f"{agent_folder}/{filename}")
        if content:
            with open(os.path.join(tmp_dir, filename), "w") as f:
                f.write(content)
            downloaded.append(filename)
    # fetch rules/error_rules.json for airflow fixer
    rules = fetch_file(f"{agent_folder}/rules/error_rules.json")
    if rules:
        os.makedirs(os.path.join(tmp_dir, "rules"), exist_ok=True)
        with open(os.path.join(tmp_dir, "rules", "error_rules.json"), "w") as f:
            f.write(rules)
    return "main.py" in downloaded, downloaded


def fix_paths(tmp_dir):
    """Fix hardcoded ~/airflow_cmd_agent paths in fixer.py to use tmp_dir."""
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


def run_agent_script(tmp_dir, stdin_input=None, timeout=1200):
    main_path = os.path.join(tmp_dir, "main.py")
    env = os.environ.copy()
    env["PYTHONPATH"] = tmp_dir
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
        return f"Error: {e}"


def run_agent(tool_name, args):
    if tool_name == "run_airflow_agent":
        port     = args.get("port", "8080")
        username = args.get("username", "admin")
        password = args.get("password", "admin")
        tmp_dir  = tempfile.mkdtemp(prefix="airflow_agent_")
        ok, downloaded = fetch_folder("airflow_agent", tmp_dir)
        if not ok:
            return f"Could not download airflow_agent/main.py from GitHub.\nDownloaded: {downloaded}"
        fix_paths(tmp_dir)
        log.info(f"Running airflow agent, downloaded: {downloaded}")
        # Pass automated answers to input() prompts: port, username, password
        stdin_input = f"{port}\n{username}\n{password}\n"
        return run_agent_script(tmp_dir, stdin_input=stdin_input, timeout=1200)

    elif tool_name == "run_hadoop_agent":
        tmp_dir = tempfile.mkdtemp(prefix="hadoop_agent_")
        ok, downloaded = fetch_folder("hadoop_agent", tmp_dir)
        if not ok:
            return f"Could not download hadoop_agent/main.py from GitHub.\nDownloaded: {downloaded}"
        env = os.environ.copy()
        env["AGENT_VERSION"] = args.get("version", "3.3.6")
        return run_agent_script(tmp_dir, timeout=600)

    elif tool_name == "run_spark_agent":
        tmp_dir = tempfile.mkdtemp(prefix="spark_agent_")
        ok, downloaded = fetch_folder("spark_agent", tmp_dir)
        if not ok:
            return f"Could not download spark_agent/main.py from GitHub.\nDownloaded: {downloaded}"
        env = os.environ.copy()
        env["AGENT_VERSION"] = args.get("version", "3.5.0")
        return run_agent_script(tmp_dir, timeout=600)

    return f"Unknown tool: {tool_name}"


def handle(req):
    method = req.get("method", "")
    rid    = req.get("id")

    if method == "initialize":
        return {"jsonrpc": "2.0", "id": rid, "result": {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "ai-agents-github-mcp", "version": "2.0.0"}
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
    log.info("ai-agents-github-mcp v2.0 started")
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

