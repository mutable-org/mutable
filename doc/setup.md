# Preliminaries

Make sure you satisfy all the [preliminaries](preliminaries.md).

# Get the Code

Clone the repository:
```
$ cd ~
$ mkdir mutable-org
$ cd mutable-org
$ git clone git@github.com:mutable-org/mutable.git
```
The above example creates the project in `~/mutable-org/mutable`.

# Build mu*t*able

We configure our project with CMake and then build it with Ninja.
In the example below, we create a build that is:
- in `Debug` mode
- with shared libraries
- with sanitizers enabled
- with sanity fields enabled (fields in data structures used for sanity checking at runtime)
- with our WebAssembly-based execution backend enabled

This is our recommended configuration for developers.
The shared-library build significantly reduces the time of repeated builds as it avoids linking with large libraries (e.g. V8 or Binaryen).
Building in `Debug` mode enables pre- and post-condition checking in the code.
Building with sanitizers helps us to detect bugs related to out-of-bounds accesses, dangling references, double-free, leaks, and undefined behavior.
Enabling our own fields for sanity checking also aids to detect some unintended behavior.

## Configure the Project
We rely on [CMake](https://cmake.org/) as build configuration system.
To configure the project, use the commands below.
In the example below, we use the [Ninja build system](https://ninja-build.org/) and the [Clang compiler](https://clang.llvm.org/).
```plain
$ cd ~/mutable-org/mutable
$ cmake -S . -B build/debug_shared \
-G Ninja \
-DCMAKE_C_COMPILER=clang \
-DCMAKE_CXX_COMPILER=clang++ \
-DCMAKE_BUILD_TYPE=Debug \
-DBUILD_SHARED_LIBS=ON \
-DENABLE_SANITIZERS=ON \
-DENABLE_SANITY_FIELDS=ON \
-DWITH_V8=ON
-- The C compiler identification is Clang 14.0.6
-- The CXX compiler identification is Clang 14.0.6
-- Check for working C compiler: /usr/bin/clang
-- Check for working C compiler: /usr/bin/clang -- works
-- Detecting C compiler ABI info
-- Detecting C compiler ABI info - done
-- Detecting C compile features
-- Detecting C compile features - done
-- Check for working CXX compiler: /usr/bin/clang++
-- Check for working CXX compiler: /usr/bin/clang++ -- works
-- Detecting CXX compiler ABI info
-- Detecting CXX compiler ABI info - done
-- Detecting CXX compile features
-- Detecting CXX compile features - done
-- Looking for pthread.h
-- Looking for pthread.h - found
-- Performing Test CMAKE_HAVE_LIBC_PTHREAD
-- Performing Test CMAKE_HAVE_LIBC_PTHREAD - Failed
-- Looking for pthread_create in pthreads
-- Looking for pthread_create in pthreads - not found
-- Looking for pthread_create in pthread
-- Looking for pthread_create in pthread - found
-- Found Threads: TRUE
-- Configuring done
-- Generating done
-- Build files have been written to: ~/mutable-org/mutable/build/debug
```
By reading the output of CMake, verify that the correct compiler (here Clang 14.0.6) is used and that threading is supported (`Found Threads: TRUE`).
Now you are ready to build!

## Build the Project
```plain
$ cmake --build build/debug_shared
```

Note, that building requires internet access, because our project depends on third parties, that are download, configured, and build when configuring / building mu*t*able for the first time.

When building with the WebAssembly-based backend enabled, building the project will take a very long time.
(This can take one hour, depending on your specs).

## Configuration Options

Below follows a list of CMake configuration options to tune the build.

### `-DBUILD_SHARED_LIBS`: Configure static or shared library build

- `-DBUILD_SHARED_LIBS=ON` -- Recommended for debug and/or developer builds.  Avoids long link times.
- `-DBUILD_SHARED_LIBS=OFF` -- Recommended for release builds.  Bundles all libraries (ours and third-parties) into the
  executables.  Reduces executable's start-up time.

### `-DENABLE_SANITIZERS`: Compile with the Address- and UndefinedBehaviorSanitizer

Clang provides a set of *sanitizers* that can be used to detect certain erroneous behaviors of the program at runtime.
Enabling a sanitizer causes the compiler to instrument the generated binary by additional error-checking code and -- depending on the sanitizer -- links the binary to a sanitizer library.

Currently, we use Clang's [AddressSanitizer](https://clang.llvm.org/docs/AddressSanitizer.html) and [UndefinedBehaviorSanitizer](https://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html).
For the UndefinedBehaviorSanitizer, we explicitly disable the `vptr` sanitizer, as this causes [linking errors with V8](https://groups.google.com/g/v8-users/c/MJztlKiWFUc/m/z3_V-SMvAwAJ).

- `-DENABLE_SANITIZERS=ON` -- Build with sanitizers.
- `-DENABLE_SANITIZERS=OFF` -- Build without sanitizers.

### `-DENABLE_SANITY_FIELDS`: Compile with custom sanity fields in data structures

Few of our data structures have fields, that are only used for sanity checking at runtime.
These fields (and the respective sanity checking code) can be enabled or disabled through a CMake option.

- `-DENABLE_SANITY_FIELDS=ON`: Compile with sanity fields and checking.
- `-DENABLE_SANITY_FIELDS=OFF`: Compile without sanity fields and checking.

**NOTE:** When compiling `libmutable` with sanity fields enabled, i.e. `-DENABLE_SANITY_FIELDS=On`, the client using `libmutable` must define `M_ENABLE_SANITY_FIELDS`.
Otherwise, there will be runtime errors: the client will see a data structure w/o the sanity fields while `libmutable` was compiled with sanity fields.


### `-DWITH_V8`: Enable WebAssembly execution backend with V8 engine.
Requirements: see [Building with V8](setup-building-with-V8.md)

- `-DBUILD_WITH_V8=ON` -- Build mu*t*able with the WebAssembly-based execution backend.

Downloads, configures, and builds [Google's V8](https://v8.dev/) JavaScript and WebAssembly engine as part of
mu*t*able's build step.
<br/>
**NOTE:** First build takes very long!

### DEPRECATED: ~~`-DWITH_SPIDERMONKEY=1`: Enable SpiderMonkey WASM platform.~~

We dropped support for using SpiderMonkey in the WebAssembly-based execution backend.

### `-DUSE_LLD`: Use LLD, a linker from the LLVM project

If you have [LLD](https://lld.llvm.org/) installed on your system, you can configure the build to use it for linking.

- `-DUSE_LLD=ON` -- Use LLD for linking.

Using LLD is likely to reduce link times over your system's standard linker.
</br>
**NOTE:** When configuring mu*t*able for the first time, we detect whether LLD is installed and set `USE_LLD` accordingly.

### EXPERIMENTAL: `-DUSE_LIBCXX`: Use LLVM's `libc++`

Configure the build to use [LLVM's implementation of the C++ standard library `libc++`](https://libcxx.llvm.org/). (This is already the default on macOS.)

- `-DUSE_LIBCXX=ON` -- Build mu*t*able with `libc++`.


# Troubleshooting
This is a list of problems we have encountered when setting up the project and how we managed to solve them.
* If `graphviz` cannot render to pdf with error message
  ```plain
  $ Error: renderer for pdf is unavailable
  ```
  install both `cairo` and `pango` and reinstall `graphviz`.

*
  ```
  clang++: error while loading shared libraries: libtinfo.so.5: cannot open shared object file: No such file or directory
  ```
  To remedy this error you must provide libncurses5 or a compatibility library.
    - On ArchLinux, you can install `aur/ncurses5-compat-libs`.
* During the CMake configure step, repository `eigen3` is cloned.  This step may time out randomly.  (It appears to be an issue of the Eigen remote repository.)  Simply repeat the step until it eventually succeeds.
