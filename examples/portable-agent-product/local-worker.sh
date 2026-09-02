#!/bin/sh
# Minimal protocol adapter: do not log stdin because it may contain private data.
payload=$(sed -n '1p')
printf '{"ok":true,"adapter":"local_process","input_received":%s}\n' "$(test -n "$payload" && printf true || printf false)"
