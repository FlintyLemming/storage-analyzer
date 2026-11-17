#!/bin/sh
exec "$(/sbin/getcfg StorageAnalyzer Install_Path -f /etc/config/qpkg.conf)/bin/storage-analyzer" cgi
