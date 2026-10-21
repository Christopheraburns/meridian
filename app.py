"""Small single-user control plane for local Docker and Cloudera AI Workbench."""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
import threading
from flask import Flask, jsonify, request, render_template_string
from meridian.generator import config_load, generate
from meridian.config import Config

app=Flask(__name__)
STATE={'status':'idle','generated':0,'target':0,'error':None,'stats':None}
LOCK=threading.Lock()
CONFIG=os.getenv('GENERATOR_CONFIG','config/default.yaml')
OUTPUT=os.getenv('GENERATOR_OUTPUT','data')
HTML='''<!doctype html><html><head><title>Meridian Financial Data Generator</title><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{font:16px system-ui;max-width:850px;margin:60px auto;padding:0 20px;color:#122237}button{background:#174f87;color:white;border:0;padding:13px 18px;border-radius:6px;cursor:pointer}pre{background:#f1f5f9;padding:20px;overflow:auto}h1{color:#174f87}</style></head><body><h1>Meridian Financial Data Generator</h1><p>Portable, deterministic synthetic payments with planted payment-routing incident.</p><p><b>Configuration:</b> {{config}}</p><button onclick="fetch('/api/start',{method:'POST'}).then(r=>r.json()).then(x=>alert(JSON.stringify(x)))">Start generation</button><h2>Job status</h2><pre id="state">Loading...</pre><script>async function refresh(){let r=await fetch('/api/status');document.getElementById('state').textContent=JSON.stringify(await r.json(),null,2)}refresh();setInterval(refresh,2500)</script></body></html>'''

@app.get('/')
def index(): return render_template_string(HTML,config=CONFIG)

@app.get('/health')
def health(): return jsonify(ok=True)

@app.get('/api/status')
def status():
    with LOCK: return jsonify(dict(STATE))

def runner(cfg):
    try:
        def progress(done,total):
            with LOCK: STATE.update(generated=done,target=total)
        stats=generate(cfg,output=OUTPUT,progress=progress)
        with LOCK: STATE.update(status='complete',stats=stats)
    except Exception as e:
        with LOCK: STATE.update(status='failed',error=str(e))

@app.post('/api/start')
def start():
    with LOCK:
        if STATE['status']=='running': return jsonify(error='Already running'),409
        try:
            cfg=config_load(CONFIG)
            if Config.parse(cfg).rows >= 50000000:
                return jsonify(error='Full-scale generation is deferred to M5; use the CLI for authorized jobs'),400
        except (ValueError, TypeError) as exc:
            return jsonify(error=str(exc)),400
        STATE.update(status='running',generated=0,target=Config.parse(cfg).rows,error=None,stats=None)
        threading.Thread(target=runner,args=(cfg,),daemon=True).start()
    return jsonify(status='started'),202

if __name__=='__main__':
    app.run(host='0.0.0.0',port=int(os.getenv('CDSW_APP_PORT',os.getenv('PORT','8080'))))
