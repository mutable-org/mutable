# Install Google `depot_tools`

## Build from Source

Clone the repository
```bash
$ git clone https://chromium.googlesource.com/chromium/tools/depot_tools.git
```
* Add the complete folder to your `$PATH`
```bash
export PATH=$PATH:/path/to/depot_tools
```

This is a condensed version of [depot_tools_tutorial(7)](https://commondatastorage.googleapis.com/chrome-infra-docs/flat/depot_tools/docs/html/depot_tools_tutorial.html#_setting_up).

## Install on ArchLinux
You can simply install the AUR package `depot-tools-git`.  The package will instruct you to update your `$PATH`:
```
>>>> Add export PATH="${PATH}:/opt/depot_tools" to .bashrc/.zshrc or setenv PATH "${PATH}:/opt/depot_tools" to ~/.tcshrc if you don't find any depot_tools related commands."
```

After the installation you must set proper permissions for `/opt/depot_tools`, e.g. via ACLs:
```bash
# groupadd depot_tools
# usermod -a -G depot_tools $(whoami)
# setfacl -m g:depot_tools:rwX /opt/depot_tools/
```
Then sign your user in to the new group with `newgrp depot_tools` or login to a new session.  Add every further user with permission to use the depot-tools to group `depot_tools`.
