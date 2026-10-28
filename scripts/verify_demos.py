"""Run legacy demos in disposable directories, preserving the user's data/."""
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    environment = dict(os.environ, PYTHONPATH=str(root), PYTHONUTF8='1')
    modules = sorted(p.stem for p in (root / 'scripts').glob('demo_*.py') if p.stem != 'demo_network_ui')
    cases = [(module, []) for module in modules] + [('demo_client_file_control', ['--receiver-cancel'])]
    for module, args in cases:
        with tempfile.TemporaryDirectory(prefix='btl-demo-') as directory:
            result = subprocess.run([sys.executable, '-m', 'scripts.'+module, *args],
                cwd=directory, env=environment, capture_output=True, text=True, encoding='utf-8', timeout=30)
            if result.returncode:
                raise RuntimeError(module + ': ' + result.stdout[-2000:] + result.stderr[-2000:])
            print('PASS', module, *args, flush=True)
    print(f'{len(cases)} demo runs PASS; only mock/local disk, not network evidence.')


if __name__ == '__main__':
    main()
