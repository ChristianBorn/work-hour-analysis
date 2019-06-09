from cx_Freeze import setup, Executable
import os


os.environ['TCL_LIBRARY'] = "D:\\ProgramData\\Anaconda3\\DLLs\\tcl86t.dll"
os.environ['TK_LIBRARY'] = "D:\\ProgramData\\Anaconda3\\DLLs\\tk86t.dll"
base = None

executables = [Executable("main.py", base=base)]

packages = ["idna"]
options = {
    'build_exe': {
        'packages': ['sqlite3', 'pandas', 'numpy', 'datetime'],
    },
}

setup(
    name = "<any name>",
    options = options,
    version = "1.0",
    description = '<any description>',
    executables = executables
)