# Storage Analyzer for Linux

An ncdu-style disk usage analyzer with a web UI.

## Install

    sudo ./install.sh

This installs the binary to /usr/local/bin, the systemd unit to
/etc/systemd/system and a default config to /etc/storage-analyzer/config
(an existing config is kept). Running it again upgrades in place.

## First start

    sudo storage-analyzer passwd --config /etc/storage-analyzer/config
    sudo systemctl enable --now storage-analyzer

Then open http://SERVER:8099 and sign in with that password.

## Uninstall

    sudo ./uninstall.sh            # keep config and data
    sudo ./uninstall.sh --purge    # also delete /etc/storage-analyzer and /var/lib/storage-analyzer

## More

See the project README (https://github.com/FlintyLemming/storage-analyzer)
for configuration options and HTTPS setup.
