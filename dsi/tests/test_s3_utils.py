import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


class TestS3OptionalDependency(unittest.TestCase):
    def test_s3_module_imports_without_boto3(self):
        script = textwrap.dedent(
            """
            import sys

            sys.modules["boto3"] = sys.modules["botocore"] = None
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
