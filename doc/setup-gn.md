# Install Standalone Google `gn` Build Configuration Tool

`gn` -- short for *generate Ninja* -- is Google's take on CMake.

## Build from Source

### Preliminaries
- Install `git`.
- Install `ninja`.
- Install a C and C++ compiler and set `CC` and `CXX` environment variables.

### Build `gn`
- Clone the repository, generate `ninja` files, and compile `gn`.
```bash
$ git clone https://gn.googlesource.com/gn
$ cd gn
$ python build/gen.py
$ ninja -C out
```
- Make the `gn` executable available in your `$PATH`, e.g.
```bash
$ cp out/gn /usr/local/bin
```
Make sure that standalone `gn` is your default version.
(`depot_tools` also includes a `gn` version, and we don't want to use that.)

- Verify installation succeeded:
```bash
$ which gn
/usr/local/bin/gn
```

This is a condensed version of [Build Google `gn` build tool standalone](https://gist.github.com/mohamed/4fa7eb75807463d4dfa3).

## Install on ArchLinux
You can simply install the package `extra/gn`.
