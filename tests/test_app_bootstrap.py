"""Exercise Workbench-style execution without a script __file__."""
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
    monkeypatch.setenv('GENERATOR_API_TOKEN','bootstrap-test-token')
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
    response=client.get('/api/config/schema',headers={'Authorization':'Bearer bootstrap-test-token'})
    assert response.status_code==200
    assert response.json['default_config']['scale']['transaction_rows']==10000


def test_missing_project_root_is_actionable(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('GENERATOR_PROJECT_ROOT',raising=False)
    with pytest.raises(RuntimeError,match='GENERATOR_PROJECT_ROOT'):
        execute_app()
