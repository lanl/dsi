import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


class TestS3OptionalDependency(unittest.TestCase):
    def test_s3_module_imports_without_boto3(self):
        script = textwrap.dedent(
            """
            import builtins

            real_import = builtins.__import__

            def block_boto3(name, *args, **kwargs):
                if name == "boto3" or name.startswith("botocore"):
                    raise ModuleNotFoundError(name)
                return real_import(name, *args, **kwargs)

            builtins.__import__ = block_boto3
            from dsi.utils.s3_utils import get_s3_client

            try:
                get_s3_client(interactive=False)
            except ImportError as exc:
                assert "boto3" in str(exc)
            else:
                raise AssertionError("missing boto3 was not reported")
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).parents[2],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
