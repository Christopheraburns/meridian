"""Exercise Workbench-style execution without a script __file__."""
import json
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT/'app.py').read_text()


def execute_app():
    namespace = {'__name__':'meridian_workbench_test'}
    exec(compile(SOURCE,'<workbench cell>','exec'),namespace)
    return namespace


@pytest.mark.parametrize('location',['root','subdirectory','explicit'])
def test_workbench_without_file(location,tmp_path,monkeypatch):
    monkeypatch.delenv('GENERATOR_PROJECT_ROOT',raising=False)
    monkeypatch.delenv('GENERATOR_CONFIG',raising=False)
    monkeypatch.setenv('GENERATOR_STATE_DIR',str(tmp_path/'state'))
    monkeypatch.setenv('GENERATOR_OUTPUT',str(tmp_path/'public'))
    monkeypatch.setenv('GENERATOR_PRIVATE_OUTPUT',str(tmp_path/'private'))
    if location=='explicit':
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv('GENERATOR_PROJECT_ROOT',str(ROOT))
    else:
        monkeypatch.chdir(ROOT if location=='root' else ROOT/'tests')
    loaded=execute_app()
    assert loaded['PROJECT_ROOT']==ROOT
    client=loaded['app'].test_client()
    assert client.get('/health').json=={'ok':True}
    assert client.get('/').status_code==200
    response=client.get('/api/config/schema')
    assert response.status_code==200
    assert response.json['default_config']['scale']['transaction_rows']==10000


def test_missing_project_root_is_actionable(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('GENERATOR_PROJECT_ROOT',raising=False)
    with pytest.raises(RuntimeError,match='GENERATOR_PROJECT_ROOT'):
        execute_app()


ENTRY_POINTS = ['cli.py','worker.py','scripts/calibrate_scenario.py','scripts/preflight.py',
                'scripts/reconcile_impala.py','scripts/validate_scenario.py','scripts/warehouse.py']


@pytest.mark.parametrize('entry',ENTRY_POINTS)
def test_entry_points_run_as_workbench_cells(entry,tmp_path,monkeypatch):
    # Workbench Jobs start in /home/cdsw with the checkout in a subdirectory, and run scripts without __file__.
    (tmp_path/'meridian').symlink_to(ROOT,target_is_directory=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('GENERATOR_PROJECT_ROOT',raising=False)
    monkeypatch.setattr('sys.path',list(__import__('sys').path))
    namespace = {'__name__':'workbench_cell'}
    exec(compile((ROOT/entry).read_text(),'<workbench cell>','exec'),namespace)
    assert Path.cwd().resolve()==ROOT
    assert any(Path(p).resolve()==ROOT/'src' for p in __import__('sys').path)


def test_job_arguments_replace_kernel_argv(tmp_path,monkeypatch):
    import sys
    (tmp_path/'meridian').symlink_to(ROOT,target_is_directory=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('sys.path',list(sys.path))
    monkeypatch.setattr('sys.argv',['/usr/local/lib/python3.12/site-packages/ipykernel_launcher.py','-f','/tmp/kernel.json'])
    out = tmp_path/'run'
    monkeypatch.setenv('MERIDIAN_ARGS',f"--config config/smoke.yaml --output '{out}/public' --private-output '{out}/private' --rows 600")
    exec(compile((ROOT/'cli.py').read_text(),'<workbench cell>','exec'),{'__name__':'__main__'})
    assert json.loads((out/'public/public_manifest.json').read_text())['rows_generated']==600
    monkeypatch.delenv('MERIDIAN_ARGS')
    monkeypatch.setattr('sys.argv',['/x/ipykernel_launcher.py','-f','/tmp/kernel.json'])
    with pytest.raises(SystemExit):
        exec(compile((ROOT/'scripts/validate_scenario.py').read_text(),'<workbench cell>','exec'),{'__name__':'__main__'})
