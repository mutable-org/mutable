# Building LLVM Clang from Source

This is a TL;DR compiled from the [documentation on building LLVM with CMake](https://llvm.org/docs/CMake.html).

Clone the repository.  Use `--branch` to directly check out the version to build.
```plain
$ git clone --branch llvmorg-16.0.6 https://github.com/llvm/llvm-project.git
$ cd llvm-project
```

Configure a release build.
Make sure to change the setting for `-DLLVM_TARGETS_TO_BUILD=` to match your architecture.
E.g., for macOS on M1, set to `AArch64`.
If you witness the build exhausting your main memory, lower the number of `-DLLVM_PARALLEL_LINK_JOBS`.
```plain
$ cmake -S llvm -B build \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DLLVM_USE_LINKER=lld \
    -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra;lld" \
    -DLLVM_ENABLE_RUNTIMES="compiler-rt" \
    -DLLVM_PARALLEL_COMPILE_JOBS=$(( $(getconf _NPROCESSORS_ONLN) - 1)) \
    -DLLVM_PARALLEL_LINK_JOBS=4 \
    -DLLVM_TARGETS_TO_BUILD="X86" \
    -DLLVM_RUNTIME_TARGETS="x86_64-unknown-linux-gnu"
```

Make sure that the CMake output includes the following lines:
```plain
-- clang project is enabled
-- clang-tools-extra project is enabled
-- compiler-rt project is enabled
...
-- lld project is enabled
```

You are now ready to build LLVM, Clang, and the other enabled projects.
This will take a while.
Even on a high-end PC with an AMD 7800X3D and 32 GiB of DDR5 memory and NVME storage it took 15 minutes.
```plain
$ cmake --build build
```

When you are done building, we are ready to install.
Here, we will install Clang and other projects to `/usr/local`, as we do not want to overwrite existing installations, that also may be managed by your systems package manager (e.g. Pacman or Homebrew).
Also, usually `/usr/local/bin` precedes `/usr/bin` in `$PATH`, and so our freshly built and installed Clang will be used by default.
If you do *not* want to install Clang system-wide and/or you do *not* want to overrule the Clang installation in `/usr/bin`, you can also install to `${HOME}/.local/`.
```plain
$ cd build
$ sudo cmake -DCMAKE_INSTALL_PREFIX=/usr/local -P cmake_install.cmake
```
