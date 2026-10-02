## Install Prerequisites

* **Ubuntu/Debian**

  ```bash
  sudo apt-get update && sudo apt-get install \
    build-essential libevent-dev zlib1g-dev libssl-dev cmake
  ```

* **CentOS/RHEL/Fedora**

  ```bash
  sudo dnf install \
    @development-tools libevent-devel zlib-devel openssl-devel cmake
  ```

---

## Installation & CMake Integration

Shared builds are available via `-DBUILD_SHARED_LIBS=ON`, but static is the default.

- Supported flags
  - -DUSE_TLS=ON, **OFF** by default (TLS support)
  - -DLIBWSC_USE_DEBUG=ON, **OFF** by default (verbose debugging, logs to stdout|stderr or syslog)
  - -DBUILD_SHARED_LIBS=ON, **OFF** by default

The easiest way is to clone the repository and use it in your cmake project via `add_sudirectory()`. You can also build a shared library:

```bash
git clone git@github.com:amigniter/libwsc.git
cd libwsc
mkdir build && cd build

# Shared library:
cmake .. \
    -DCMAKE_BUILD_TYPE=Release  \
    -DUSE_TLS=ON                \
    -DLIBWSC_USE_DEBUG=ON       \
    -DBUILD_SHARED_LIBS=ON
make
sudo make install
```

### Integration

**In-tree**

```cmake
# Top-level CMakeLists.txt
set(USE_TLS ON CACHE BOOL "" FORCE)
add_subdirectory(path/to/libwsc)

add_executable(myapp main.cpp)
target_link_libraries(myapp PRIVATE libwsc)
```

**Installed package**

```cmake
# After running install above:
find_package(libwsc REQUIRED)

add_executable(myapp main.cpp)
target_link_libraries(myapp PRIVATE libwsc::libwsc)
```

---