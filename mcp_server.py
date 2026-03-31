from fastmcp import FastMCP
import os, subprocess, tempfile, logging
import urllib.request

logging.basicConfig(
    filename=os.path.expanduser("~/ai_agents_mcp.log"),
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

GITHUB_USER   = "khizar-25"
GITHUB_REPO   = "ai_agents"
GITHUB_BRANCH = "main"
GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN", "")
RAW_BASE = f"https://raw.githubusercontent.com/{GITHUB_USER}/{GITHUB_REPO}/{GITHUB_BRANCH}"

mcp = FastMCP("AI Agents Server 🚀")

# ── Helper ──────────────────────────────────────────

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

def fetch_and_run(folder, files, tmp_dir, stdin_input=None, extra_env=None, timeout=900):
    for fn in files:
        c = fetch_file(f"{folder}/{fn}")
        if c:
            with open(os.path.join(tmp_dir, fn), "w") as f:
                f.write(c)
    rules = fetch_file(f"{folder}/rules/error_rules.json")
    if rules:
        os.makedirs(os.path.join(tmp_dir, "rules"), exist_ok=True)
        with open(os.path.join(tmp_dir, "rules", "error_rules.json"), "w") as f:
            f.write(rules)
    env = os.environ.copy()
    env["PYTHONPATH"] = tmp_dir
    if extra_env:
        env.update(extra_env)
    try:
        r = subprocess.run(
            ["python", os.path.join(tmp_dir, "main.py")],
            input=stdin_input, capture_output=True, text=True,
            timeout=timeout, env=env, cwd=tmp_dir
        )
        out = r.stdout or ""
        if r.stderr: out += f"\n[stderr]:\n{r.stderr}"
        if r.returncode != 0: out = f"Agent exited {r.returncode}\n{out}"
        return out.strip() or "Agent completed."
    except subprocess.TimeoutExpired:
        return f"Timed out after {timeout}s."
    except Exception as e:
        return f"Error: {e}"

def build_worker_env(args, base_env):
    count = int(args.get("worker_count", "0"))
    base_env["AGENT_WORKER_COUNT"] = str(count)
    for i in range(1, count + 1):
        base_env[f"AGENT_WORKER_{i}_IP"]   = args.get(f"worker_{i}_ip", "")
        base_env[f"AGENT_WORKER_{i}_USER"] = args.get(f"worker_{i}_user", "")
        base_env[f"AGENT_WORKER_{i}_PASS"] = args.get(f"worker_{i}_pass", "")
    return base_env

# ── Tools ────────────────────────────────────────────

@mcp.tool
def run_hadoop_agent(
    master_ip: str,
    master_user: str,
    master_pass: str,
    worker_count: str = "0",
    worker_1_ip: str = "", worker_1_user: str = "", worker_1_pass: str = "",
    worker_2_ip: str = "", worker_2_user: str = "", worker_2_pass: str = "",
    worker_3_ip: str = "", worker_3_user: str = "", worker_3_pass: str = ""
) -> str:
    """Install Apache Hadoop 3.4.2 on master and worker nodes."""
    env = {
        "AGENT_VERSION":     "3.4.2",
        "AGENT_MASTER_IP":   master_ip,
        "AGENT_MASTER_USER": master_user,
        "AGENT_MASTER_PASS": master_pass,
    }
    args = {
        "worker_count": worker_count,
        "worker_1_ip": worker_1_ip, "worker_1_user": worker_1_user, "worker_1_pass": worker_1_pass,
        "worker_2_ip": worker_2_ip, "worker_2_user": worker_2_user, "worker_2_pass": worker_2_pass,
        "worker_3_ip": worker_3_ip, "worker_3_user": worker_3_user, "worker_3_pass": worker_3_pass,
    }
    env = build_worker_env(args, env)
    return fetch_and_run("hadoop_agent", ["main.py"], tempfile.mkdtemp(prefix="hadoop_"), extra_env=env, timeout=900)

@mcp.tool
def run_spark_agent(
    master_ip: str,
    master_user: str,
    master_pass: str,
    worker_count: str = "0",
    version: str = "3.5.0",
    worker_1_ip: str = "", worker_1_user: str = "", worker_1_pass: str = "",
    worker_2_ip: str = "", worker_2_user: str = "", worker_2_pass: str = ""
) -> str:
    """Install Apache Spark on master and worker nodes."""
    env = {
        "AGENT_VERSION":     version,
        "AGENT_MASTER_IP":   master_ip,
        "AGENT_MASTER_USER": master_user,
        "AGENT_MASTER_PASS": master_pass,
    }
    args = {
        "worker_count": worker_count,
        "worker_1_ip": worker_1_ip, "worker_1_user": worker_1_user, "worker_1_pass": worker_1_pass,
        "worker_2_ip": worker_2_ip, "worker_2_user": worker_2_user, "worker_2_pass": worker_2_pass,
    }
    env = build_worker_env(args, env)
    return fetch_and_run("spark_agent", ["main.py"], tempfile.mkdtemp(prefix="spark_"), extra_env=env, timeout=600)

@mcp.tool
def run_airflow_agent(
    port: str,
    username: str,
    password: str,
    action: str = "reinstall"
) -> str:
    """Install and configure Apache Airflow on this machine."""
    choice   = {"verify": "1", "repair": "2", "reinstall": "3"}.get(action.lower(), "3")
    stdin    = f"{port}\n{username}\n{password}\n{choice}\n{choice}\n"
    files    = ["main.py","config.py","runner.py","checks.py","installer.py",
                "configurator.py","fixer.py","verifier.py","ai_analyzer.py"]
    return fetch_and_run("airflow_agent", files, tempfile.mkdtemp(prefix="airflow_"), stdin_input=stdin, timeout=1200)

# ── Run ──────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run(transport="streamable-http", port=8000)
