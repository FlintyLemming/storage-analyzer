#!/bin/sh
CONF=/etc/config/qpkg.conf
QPKG_NAME="StorageAnalyzer"
QPKG_ROOT=$(/sbin/getcfg $QPKG_NAME Install_Path -f $CONF)
BIN="$QPKG_ROOT/bin/storage-analyzer"
CGI_LINK="/home/httpd/cgi-bin/qpkg/$QPKG_NAME"
export QNAP_QPKG=$QPKG_NAME

case "$1" in
  start)
    ENABLED=$(/sbin/getcfg $QPKG_NAME Enable -u -d FALSE -f $CONF)
    if [ "$ENABLED" != "TRUE" ]; then
        echo "$QPKG_NAME is disabled."
        exit 1
    fi
    /bin/mkdir -p "$QPKG_ROOT/data/scans" "$QPKG_ROOT/data/run" "$QPKG_ROOT/data/logs" /home/httpd/cgi-bin/qpkg
    /bin/chmod 700 "$QPKG_ROOT/data" "$QPKG_ROOT/data/scans"
    /bin/rm -rf "$CGI_LINK"
    /bin/ln -sf "$QPKG_ROOT/web" "$CGI_LINK"
    "$BIN" init-start >>"$QPKG_ROOT/data/logs/service.log" 2>&1
    ;;

  stop)
    [ -x "$BIN" ] && "$BIN" init-stop >>"$QPKG_ROOT/data/logs/service.log" 2>&1
    /bin/rm -rf "$CGI_LINK"
    ;;

  restart)
    $0 stop
    $0 start
    ;;

  remove)
    [ -x "$BIN" ] && "$BIN" init-stop >/dev/null 2>&1
    /bin/rm -rf "$CGI_LINK"
    ;;

  *)
    echo "Usage: $0 {start|stop|restart|remove}"
    exit 1
esac

exit 0
