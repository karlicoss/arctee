Stdout gets written into a file, stderr gets passes through

# Usage
Add to anacron:
```
1         123     backup-whatever       /path/to/backup-wrapper.py --dir=/backups/whatever --prefix=whatever.json --command="/bin/backup-whatever.sh"
```

# Prerequisites
* optional: coloredlogs
