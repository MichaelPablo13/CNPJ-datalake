import importlib.util
import os
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock


def _load_dag_module():
    airflow_module = types.ModuleType("airflow")
    decorators_module = types.ModuleType("airflow.decorators")
    operators_module = types.ModuleType("airflow.operators")
    python_module = types.ModuleType("airflow.operators.python")

    class DummyTask:
        def __init__(self, task_id="dummy"):
            self.task_id = task_id

        def __rshift__(self, other):
            return other

    def fake_dag(*args, **kwargs):
        def decorator(fn):
            def wrapped(*f_args, **f_kwargs):
                return None
            return wrapped
        return decorator

    def fake_task(fn=None, **kwargs):
        def decorator(inner):
            def wrapped(*f_args, **f_kwargs):
                return DummyTask(getattr(inner, "__name__", "task"))
            return wrapped
        return decorator(fn) if fn is not None else decorator

    decorators_module.dag = fake_dag
    decorators_module.task = fake_task
    python_module.get_current_context = lambda: {}

    sys.modules.setdefault("airflow", airflow_module)
    sys.modules["airflow.decorators"] = decorators_module
    sys.modules["airflow.operators"] = operators_module
    sys.modules["airflow.operators.python"] = python_module

    path = Path("d:/Projetos/CNPJ-DataLake/services/airflow/dags/cnpj_dataset_dags.py")
    spec = importlib.util.spec_from_file_location("test_dag_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class DagLogicTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = _load_dag_module()

    def test_conf_overrides_env_and_logical_date(self):
        context = {
            "dag_run": types.SimpleNamespace(conf={"data_version": "2026-04"}),
            "logical_date": datetime(2026, 6, 1),
        }
        with mock.patch.object(self.module, "get_current_context", return_value=context), \
            mock.patch.dict(os.environ, {"INGESTION_DATA_MONTH": "2026-03"}, clear=False):
            self.assertEqual(self.module._resolve_data_version_from_context(), "2026-04")

    def test_ingestion_data_month_env_is_used(self):
        context = {
            "dag_run": types.SimpleNamespace(conf={}),
            "logical_date": datetime(2026, 6, 1),
        }
        with mock.patch.object(self.module, "get_current_context", return_value=context), \
            mock.patch.dict(os.environ, {"INGESTION_DATA_MONTH": "2026-03"}, clear=False):
            self.assertEqual(self.module._resolve_data_version_from_context(), "2026-03")


if __name__ == "__main__":
    unittest.main()