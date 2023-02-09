# Preliminaries

This page summarizes the requirements to configure, build, and run mu*t*able, and lists the dependencies of mu*t*able.

## Tools
Install the following tools:
* Git
* CMake (3.20 or newer)
* GNU Make / Ninja / some other build tool of your choice
* C/C++ Compiler with C++20 support (preferably Clang 14 or newer)
* Doxygen (to generate the documentation)
* Perl (for our git-hooks)

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

#### Python Libraries

We implement our own testing and benchmarking frameworks in Python.  Install the following dependencies.

##### For testing:
```
$ pip install --user --upgrade \
    colorama \
    pyyaml \
    tqdm \
    yamale
```

##### For benchmarking:
```
$ pip install --user --upgrade \
    gitpython \
    pandas \
    pyyaml \
    sqlobject \
    tqdm \
    yamale
```

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
