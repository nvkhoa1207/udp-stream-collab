import os
import subprocess
import sys
import unittest


class CliTests(unittest.TestCase):
    def test_help_works_when_windows_pipe_defaults_to_cp1252(self):
        environment = dict(os.environ, PYTHONIOENCODING='cp1252')
        for module in ('server.main', 'client.main'):
            with self.subTest(module=module):
                result = subprocess.run([sys.executable, '-m', module, '--help'],
                    capture_output=True, env=environment, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr.decode('utf-8', errors='replace'))
                self.assertIn('usage:', result.stdout.decode('utf-8'))
