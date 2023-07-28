# Preliminaries

This page summarizes the requirements to configure, build, and run mu*t*able, and lists the dependencies of mu*t*able.

## Tools
Install the following tools:
* Git
* CMake (3.20 or newer)
* GNU Make / Ninja / some other build tool of your choice
* Clang 16 or newer (see [Build Clang from Source](build-clang-from-source.md))
* Doxygen (to generate the documentation)
* Perl (for our git-hooks)
* Pipenv (to set up a virtual Python environment and manage packages)

Further, if you wish to build mu*t*able with its WebAssembly-based backend (which is the default), you need to install the following Google tools as well:
* [`depot_tools`](setup-depot_tools.md)
* [`gn`](setup-gn.md)

## Dependencies

mu*t*able depends on several third-parties.
We ship most of them indirectly through CMake's "external projects".
However, some third-parties are required to be installed on your system.

### Mandatory Dependencies

Below is a list of **mandatory** dependencies, that **must be installed**:

- Boost libraries and development header files.
    - On ArchLinux: `pacman -S boost`
- Graphviz development headers
    - Make sure `gvc.h` is found in one your system include directories.
    - On ArchLinux: `pacman -S graphviz`

### Optional Dependencies

The dependencies listed below are **optional**.  Having them available on your system provides new features to mu*t*able or improves working on mu*t*able.

#### Graphviz

Doxygen can use Graphviz to render inheritance and collaboration diagrams.
* At least `graphviz>=1.8.10` is required by our `Doxyfile`.  If you have an older version set `DOT_MULTI_TARGETS = NO`.
* On ArchLinux: `pacman -S graphviz`
* On macOS: `brew install graphviz`.

#### Python Environment & Packages

We implement our own testing and benchmarking frameworks in Python.  We manage our required packages with Pipenv and package versions are specified in the `Pipfile.lock` file.  Ensure that you have a Python provider installed, e.g. `pyenv` or the currently required version of Python.

To install our required packages, run `pipenv sync`.

#### Building with the WebAssembly-based Execution Backend

See [Building with V8](setup-building-with-V8.md).



## Install our git-hooks

We use `perl` in our git-hooks. Make sure it is in your `$PATH`.
We implement custom git-hooks to enforce code quality.
You **must** install them locally in your repo with the following commands:
```
$ cd .git/
$ rm -fr hooks/
$ ln -s ../hooks
```
This way, our hooks are properly installed and contained in version control.

## Troubleshooting

### Pipenv

- On macOS, setting up the Pipenv virtual environment can fail because of a build error in the  `pygraphviz` setup.
  If the installation of `pygraphviz` with `pipenv` fails with the following error:
  ```plain
  fatal error: 'graphviz/cgraph.h' file not found
  ```
  then the problem is that the required header file is not installed in one of the compiler's searched include directories.
  To resolve the problem, we must manually provide the include directory to the build of `pygraphviz`.

  1. Get the current install directory of Graphviz with `brew info graphviz`.
  1. Run the following command to build `pygraphviz` with the additional include directory.  Replace `<VERSION>` with your actual version of Graphviz.
     ```plain
     $ export GRAPHVIZ_DIR="/usr/local/Cellar/graphviz/<VERSION>"
     $ pipenv run pip install pygraphviz --global-option=build_ext --global-option="-I$GRAPHVIZ_DIR/include" --global-option="-L$GRAPHVIZ_DIR/lib"
     ```
  1. Run `pipenv sync` as usual.

