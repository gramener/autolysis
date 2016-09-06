import os
import sys

# Set the global variable DATA_DIR
if sys.platform == 'linux2' or sys.platform == 'cygwin':
    DATA_DIR = os.path.expanduser('~/.config/autolysis')
elif sys.platform == 'win32':
    DATA_DIR = os.path.join(os.environ['LOCALAPPDATA'], 'autolysis')
elif sys.platform == 'darwin':
    DATA_DIR = os.path.expanduser('~/Library/Application Support/autolysis')
else:
    DATA_DIR = os.path.abspath('.')
